import pandas as pd
import requests
import time
import base64
from datetime import datetime, timedelta
from io import BytesIO
from PIL import Image
from countryinfo import CountryInfo
import os

from config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, DATA_DIR

class SpotifyAPI:
    """Class to handle Spotify API authentication and requests"""
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_expiry = None
        
    def get_token(self):
        """Generate or return existing valid token"""
        if self.token and self.token_expiry > datetime.now():
            return self.token
            
        # Prepare authentication string
        auth_string = f"{self.client_id}:{self.client_secret}"
        auth_bytes = auth_string.encode('utf-8')
        auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')
        
        url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": f"Basic {auth_base64}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}
        
        result = requests.post(url, headers=headers, data=data)
        if result.status_code == 200:
            json_result = result.json()
            self.token = json_result.get('access_token')
            self.token_expiry = datetime.now() + timedelta(seconds=json_result.get('expires_in', 3600))
            return self.token
        else:
            raise Exception(f"Failed to get token: {result.status_code}")
            
    def search_artist(self, artist_name):
        """Search for an artist and return their details"""
        token = self.get_token()
        
        url = "https://api.spotify.com/v1/search"
        headers = {
            "Authorization": f"Bearer {token}"
        }
        params = {
            "q": artist_name,
            "type": "artist",
            "limit": 1  # Get only the top result
        }
        
        result = requests.get(url, headers=headers, params=params)
        if result.status_code == 200:
            json_result = result.json()
            artists = json_result.get('artists', {}).get('items', [])
            return artists[0] if artists else None
        else:
            print(f"Error searching for artist: {result.status_code}")
            return None


class ArtistDataCollector:
    """Main class to collect and process artist data"""
    
    def __init__(self, tag_query, number_of_artists):
        self.tag_query = tag_query
        self.number_of_artists = number_of_artists
        self.spotify = SpotifyAPI(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
        
        # Ensure data directory exists
        os.makedirs(DATA_DIR, exist_ok=True)
        
        # Progress tracking
        self.progress_callback = None
        self.progress_message = ""
    
    def set_progress_callback(self, callback):
        """Set callback function to update progress"""
        self.progress_callback = callback
    
    def update_progress(self, message):
        """Update progress message and call callback if set"""
        self.progress_message = message
        if self.progress_callback:
            self.progress_callback(message)
        else:
            print(message)
    
    def fetch_artists(self):
        """Fetch artists from MusicBrainz based on tag"""
        self.update_progress(f"Fetching artists with tag: {self.tag_query}...")
        
        fetched_artists = []
        for offset in range(2, self.number_of_artists + 2, 100):
            url = f"https://musicbrainz.org/ws/2/artist?query=tag:{self.tag_query}&limit=100&offset={offset}&fmt=json"
            response = requests.get(url)

            if response.status_code == 200 and response.content:
                try:
                    data = response.json()
                    batch_count = len(data.get('artists', []))
                    fetched_artists.extend(data.get('artists', []))
                    self.update_progress(f"Fetched {batch_count} artists from offset {offset}.")
                except ValueError:
                    self.update_progress("Response content is not valid JSON.")
            else:
                self.update_progress(f"No content or bad response: {response.status_code}")
                break
                
            # Be nice to the API
            time.sleep(1)

        self.update_progress(f"Total artists fetched: {len(fetched_artists)}")
        return fetched_artists
    
    def process_artist_data(self, fetched_artists):
        """Process and clean artist data"""
        self.update_progress("Processing artist data...")
        
        # Convert to DataFrame
        df = pd.json_normalize(fetched_artists)
        
        if len(df) == 0:
            self.update_progress("No artists found for this tag query.")
            return None
            
        # Set index and sort
        df = df.set_index('id').sort_values(by='score', ascending=False)
        
        # Extract features
        df['Country_name'] = df['area.name']
        df['Begin_date'] = df['life-span.begin']
        df['End_date'] = df['life-span.end']
        df['Ended'] = df['life-span.ended'].replace({None: False, True: True})
        
        # Function to extract year from date string
        def extract_year(date_str):
            if pd.isna(date_str):
                return None
            year = str(date_str)[:4]
            return int(year) if year.isdigit() else None
        
        # Process dates
        df['Year_formed'] = df['Begin_date'].apply(extract_year).astype('Int64')
        df['Year_disbanded'] = df['End_date'].apply(extract_year).astype('Int64')
        df['Lifespan'] = df['Year_disbanded'] - df['Year_formed']
        
        # Filter non-English names
        is_english = df['name'].apply(lambda x: str(x).isascii())
        df = df[is_english]
        
        # Select relevant columns
        artist_df = df[['name', 'Country_name', 'Year_formed', 'Year_disbanded', 'Ended', 'Lifespan']].copy()
        
        # Filter for valid countries
        self.update_progress("Filtering for valid countries...")
        countries_df = self.get_country_data()
        valid_countries = countries_df['Country_name'].str.lower()
        artist_df = artist_df[artist_df['Country_name'].str.lower().isin(valid_countries)].copy()
        
        # Fix ended status
        artist_df.loc[artist_df['Year_disbanded'].isna(), 'Ended'] = False
        
        self.update_progress(f"Processed data: {len(artist_df)} artists after cleaning")
        return artist_df, countries_df
    
    def get_country_data(self):
        """Get country information from CountryInfo"""
        self.update_progress("Fetching country data...")
        fetch_countries = CountryInfo().all()
        countries_df = pd.DataFrame(fetch_countries).T
        countries_df.reset_index(inplace=True)
        countries_df = countries_df[['name', 'area', 'capital', 'population', 'region']]
        countries_df.rename(columns={'name': 'Country_name'}, inplace=True)
        return countries_df
    
    def enrich_with_spotify(self, artist_df):
        """Add Spotify data to artist DataFrame"""
        self.update_progress("Enriching with Spotify data...")
        
        # Initialize new columns
        artist_df['spotify_followers'] = None
        artist_df['spotify_url'] = None
        artist_df['spotify_popularity'] = None
        artist_df['spotify_image'] = None
        
        # Process each artist
        success_count = 0
        for idx, row in artist_df.iterrows():
            artist_name = row['name']
            result = self.spotify.search_artist(artist_name)
            
            if result:
                artist_df.at[idx, 'spotify_followers'] = int(result['followers']['total'])
                artist_df.at[idx, 'spotify_url'] = result['external_urls']['spotify']
                artist_df.at[idx, 'spotify_popularity'] = int(result['popularity'])
                
                # Safely get the first image URL if available
                if result.get('images') and len(result['images']) > 0:
                    artist_df.at[idx, 'spotify_image'] = result['images'][0]['url']
                
                success_count += 1
                if success_count % 10 == 0:
                    self.update_progress(f"Found Spotify data for {success_count} artists...")
            
            # Add a small delay to avoid rate limiting
            time.sleep(0.1)
        
        # Convert to appropriate types
        artist_df['spotify_followers'] = pd.to_numeric(artist_df['spotify_followers'], errors='coerce')
        artist_df['spotify_popularity'] = pd.to_numeric(artist_df['spotify_popularity'], errors='coerce')
        
        self.update_progress(f"Spotify enrichment complete. Found data for {success_count} artists.")
        return artist_df
    
    def get_image_bytes(self, image_url, max_width=300):
        """Fetch and resize an image, returning bytes"""
        if not image_url:
            return None
        
        try:
            # Add timeout to prevent hanging
            response = requests.get(image_url, timeout=10)
            
            if response.status_code == 200:
                # Check if content is actually an image
                content_type = response.headers.get('Content-Type', '')
                if not content_type.startswith('image/'):
                    self.update_progress(f"Warning: URL doesn't contain an image: {image_url}")
                    return None
                    
                # Open and resize image
                image = Image.open(BytesIO(response.content))
                
                # Calculate new height maintaining aspect ratio
                width_percent = max_width / float(image.size[0])
                new_height = int(float(image.size[1]) * width_percent)
                
                # Resize image
                image = image.resize((max_width, new_height), Image.Resampling.LANCZOS)
                
                # Convert back to bytes
                img_byte_arr = BytesIO()
                image.save(img_byte_arr, format='PNG')
                img_byte_arr.seek(0)
                return img_byte_arr
            return None
        except requests.exceptions.Timeout:
            self.update_progress(f"Timeout while fetching image: {image_url}")
            return None
        except Exception as e:
            self.update_progress(f"Error processing image: {e}")
            return None
    
    def collect_all_data(self):
        """Run the full data collection pipeline"""
        self.update_progress(f"Starting data collection for {self.tag_query} (up to {self.number_of_artists} artists)...")
        
        # Step 1: Fetch artists from MusicBrainz
        fetched_artists = self.fetch_artists()
        
        # Step 2: Process artist data and get country info
        artist_df, countries_df = self.process_artist_data(fetched_artists)
        if artist_df is None:
            return None, None
            
        # Step 3: Enrich with Spotify data
        artist_df = self.enrich_with_spotify(artist_df)
        
        # Step 4: Save data to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{DATA_DIR}/artists_{self.tag_query.replace(' ', '_')}_{self.number_of_artists}_{timestamp}.csv"
        artist_df.to_csv(filename)
        self.update_progress(f"Data saved to {filename}")
        
        return artist_df, countries_df