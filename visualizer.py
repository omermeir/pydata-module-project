import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import plotly.express as px
from io import BytesIO

class ArtistVisualizer:
    """Generate visualizations from artist data"""
    
    def __init__(self, artist_df, countries_df, tag_query):
        self.artist_df = artist_df
        self.countries_df = countries_df
        self.tag_query = tag_query
        
        # Define color schemes
        self.colors_sunset = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEEAD',
                            '#D4A5A5', '#9B7EDE', '#FFB174', '#3F7CAC', '#EE6E73']
    
    def create_summary_text(self):
        """Create summary statistics text for display"""
        # Artist Statistics
        total_artists = len(self.artist_df)
        active_artists = (~self.artist_df['Ended']).sum()
        disbanded_artists = self.artist_df['Ended'].sum()
        active_percent = 100 * active_artists / total_artists if total_artists else 0
        disbanded_percent = 100 * disbanded_artists / total_artists if total_artists else 0
        
        avg_lifespan = self.artist_df['Lifespan'].mean()
        
        # Geographic Distribution
        countries_count = self.artist_df['Country_name'].nunique()
        top_countries = self.artist_df['Country_name'].value_counts().head(3)
        top_countries_str = ", ".join([f"{country} ({count})" for country, count in top_countries.items()])
        
        # Artists per million calculation - Add proper conversion to numeric
        merged_df = pd.merge(self.artist_df, self.countries_df, on='Country_name', how='left')
        artist_per_country = merged_df.groupby('Country_name')['name'].count().reset_index()
        artist_per_country = artist_per_country.rename(columns={'name': 'ArtistsCount'})
        artist_per_country = artist_per_country.merge(self.countries_df[['Country_name', 'population']], 
                                                    on='Country_name', how='left')
        
        # Convert population to numeric first
        artist_per_country['population'] = pd.to_numeric(artist_per_country['population'], errors='coerce')
        
        # Calculate with proper conversion
        artist_per_country['ArtistsPerMillion'] = pd.to_numeric(
            (artist_per_country['ArtistsCount'] / artist_per_country['population']) * 1_000_000,
            errors='coerce'
        )
        
        # Filter to reasonable counts and get top 3
        filtered_countries = artist_per_country[artist_per_country['ArtistsCount'] >= 3].dropna(subset=['ArtistsPerMillion'])
        
        # Use sort_values instead of nlargest
        top_per_million = filtered_countries.sort_values('ArtistsPerMillion', ascending=False).head(3)
        
        # Safely format the output string
        if len(top_per_million) > 0:
            top_per_million_str = ", ".join([f"{country} ({rate:.1f})" for country, rate 
                                            in zip(top_per_million['Country_name'], 
                                                top_per_million['ArtistsPerMillion'])])
        else:
            top_per_million_str = "No data available"
        
        # Spotify Performance
        spotify_profiles = self.artist_df['spotify_followers'].notna().sum()
        spotify_percent = 100 * spotify_profiles / total_artists if total_artists else 0
        avg_followers = self.artist_df['spotify_followers'].mean()
        avg_popularity = self.artist_df['spotify_popularity'].mean()
        
        # Timeline
        years = self.artist_df['Year_formed'].dropna()
        if len(years) >= 2:
            bins = pd.cut(years, bins=5)
            period_counts = bins.value_counts().sort_index()
            peak_period = period_counts.idxmax()
            peak_period_str = f"{peak_period.left:.0f}-{peak_period.right:.0f}"
        else:
            peak_period_str = "Insufficient data"
        
        # Find oldest active artist
        active_artists_df = self.artist_df[~self.artist_df['Ended']]
        if len(active_artists_df) > 0:
            oldest = active_artists_df.nsmallest(1, 'Year_formed')
            oldest_artist = f"{oldest['name'].iloc[0]} ({oldest['Year_formed'].iloc[0]})"
        else:
            oldest_artist = "None found"
        
        # Create formatted summary
        summary = f"📊 {self.tag_query.upper()} ANALYSIS\n\n"
        
        summary += "• Artist Statistics:\n"
        summary += f"  - Total artists analyzed: {total_artists}\n"
        summary += f"  - Active artists: {active_artists} ({active_percent:.1f}%)\n"
        summary += f"  - Disbanded artists: {disbanded_artists} ({disbanded_percent:.1f}%)\n"
        if not pd.isna(avg_lifespan):
            summary += f"  - Average artist lifespan: {avg_lifespan:.1f} years\n"
        
        summary += "\n• Geographic Distribution:\n"
        summary += f"  - Artists found in {countries_count} countries\n"
        summary += f"  - Top countries: {top_countries_str}\n"
        if top_per_million_str:
            summary += f"  - Highest artists per million: {top_per_million_str}\n"
        
        summary += "\n• Spotify Performance:\n"
        summary += f"  - Artists with Spotify profiles: {spotify_profiles} ({spotify_percent:.1f}%)\n"
        if not pd.isna(avg_followers):
            summary += f"  - Average followers: {avg_followers:,.0f}\n"
        if not pd.isna(avg_popularity):
            summary += f"  - Average popularity score: {avg_popularity:.1f}/100\n"
        
        summary += "\n• Timeline:\n"
        summary += f"  - Peak formation period: {peak_period_str}\n"
        summary += f"  - Oldest active artist: {oldest_artist}"
        
        return summary
    
    def plot_top_artists_popularity(self):
        """Create plot of top artists by popularity"""
        top_artists = self.artist_df.sort_values('spotify_popularity', ascending=False).head(10)
        
        plt.figure(figsize=(8, 10))
        plt.bar(top_artists['name'], top_artists['spotify_popularity'], color=self.colors_sunset)
        plt.title(f'Top 10 {self.tag_query} Artists by Spotify Popularity', fontsize=14, pad=15)
        plt.ylabel('Popularity', fontsize=12)
        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.yticks([])  # removes y values from plot
        plt.tight_layout()
        
        # Convert to bytes
        img_bytes = BytesIO()
        plt.savefig(img_bytes, format='PNG')
        img_bytes.seek(0)
        plt.close()
        
        return img_bytes
    
    def plot_year_distribution(self):
        """Create histogram of formation years"""
        plt.figure(figsize=(8, 10))
        self.artist_df['Year_formed'].plot(
            kind='hist',
            bins=20,
            color='mediumseagreen',
            edgecolor='black'
        )
        plt.title(f'Distribution of {self.tag_query} Artists by Year Formed', fontsize=14, pad=15)
        plt.xlabel('Year Formed', fontsize=12)
        plt.ylabel('Number of Artists', fontsize=12)
        plt.tight_layout()
        
        # Convert to bytes
        img_bytes = BytesIO()
        plt.savefig(img_bytes, format='PNG')
        img_bytes.seek(0)
        plt.close()
        
        return img_bytes
    
    def plot_active_artists_map(self):
        """Create choropleth map of active artists by country"""
        try:
            active_artists = self.artist_df[self.artist_df['Ended'] == False]
            active_counts = active_artists['Country_name'].value_counts().reset_index()
            active_counts.columns = ['Country_name', 'ActiveBands']
            
            fig = px.choropleth(
                active_counts,
                locations="Country_name",
                locationmode="country names",
                color="ActiveBands",
                color_continuous_scale='Viridis',
                title=f"Number of Active {self.tag_query} Artists by Country",
                labels={'ActiveBands': 'Active Artists'},
                width=800,
                height=400
            )
            fig.update_geos(showcoastlines=True, showland=True, landcolor="#FFFFFF")
            fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
            
            # Convert to bytes
            img_bytes = BytesIO()
            fig.write_image(img_bytes, format='PNG')
            img_bytes.seek(0)
            
            return img_bytes
        except Exception as e:
            # Create a fallback plot with matplotlib if plotly fails
            plt.figure(figsize=(8, 10))
            plt.text(0.5, 0.5, f"Could not generate map: {str(e)}", 
                     ha='center', va='center', fontsize=12, wrap=True)
            plt.tight_layout()
            
            img_bytes = BytesIO()
            plt.savefig(img_bytes, format='PNG')
            img_bytes.seek(0)
            plt.close()
            
            return img_bytes
    
    def plot_artists_per_million(self):
        """Create bar chart of artists per million population"""
        merged_df = pd.merge(self.artist_df, self.countries_df, on='Country_name', how='left')
        
        artist_per_country = merged_df.groupby('Country_name')['name'].count().reset_index()
        artist_per_country = artist_per_country.rename(columns={'name': 'ArtistsCount'})
        artist_per_country = artist_per_country.merge(self.countries_df[['Country_name', 'population']], 
                                              on='Country_name', how='left')
        
        # Convert population to numeric, handling errors
        artist_per_country['population'] = pd.to_numeric(artist_per_country['population'], errors='coerce')
        
        # Calculate artists per million and ensure it's numeric
        artist_per_country['ArtistsPerMillion'] = pd.to_numeric(
            (artist_per_country['ArtistsCount'] / artist_per_country['population']) * 1_000_000,
            errors='coerce'
        )
        
        # Filter out rows with NaN values (invalid calculations)
        artist_per_country = artist_per_country.dropna(subset=['ArtistsPerMillion'])
        
        # Filter to include only countries with at least 3 artists
        artist_per_country = artist_per_country[artist_per_country['ArtistsCount'] >= 3]
        
        # Take top 10 countries using sort_values instead of nlargest
        top_countries = artist_per_country.sort_values('ArtistsPerMillion', ascending=False).head(10)
        
        plt.figure(figsize=(8, 10))
        plt.bar(top_countries['Country_name'], top_countries['ArtistsPerMillion'], color=self.colors_sunset)
        plt.title(f'{self.tag_query} Artists per Million People', fontsize=14, pad=15)
        plt.ylabel('Artists per Million', fontsize=12)
        plt.xlabel('Country', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Convert to bytes
        img_bytes = BytesIO()
        plt.savefig(img_bytes, format='PNG')
        img_bytes.seek(0)
        plt.close()
        
        return img_bytes
    
    def plot_top_artists_followers(self):
        """Create bar chart of top artists by followers"""
        # Filter out rows with NaN spotify_followers
        valid_df = self.artist_df.dropna(subset=['spotify_followers'])
        
        # If no valid data, return empty plot with message
        if len(valid_df) == 0:
            plt.figure(figsize=(8, 10))
            plt.text(0.5, 0.5, "No Spotify follower data available", 
                     ha='center', va='center', fontsize=14)
            plt.tight_layout()
            
            # Convert to bytes
            img_bytes = BytesIO()
            plt.savefig(img_bytes, format='PNG')
            img_bytes.seek(0)
            plt.close()
            return img_bytes
        
        plt.figure(figsize=(8, 10))
        top_10_by_followers = valid_df['spotify_followers'].sort_values(ascending=False).head(10)
        top_10_artists = valid_df.loc[top_10_by_followers.index]
        
        # Convert followers to millions for easier reading
        followers_in_millions = top_10_artists['spotify_followers'] / 1_000_000
        
        plt.bar(
            top_10_artists['name'],
            followers_in_millions,
            color=self.colors_sunset,
        )
        
        # Add value labels on top of each bar
        for i, v in enumerate(followers_in_millions):
            plt.text(i, v, f'{v:.1f}M', ha='center', va='bottom')
        
        plt.title(f'Top 10 {self.tag_query} Artists by Spotify Followers', fontsize=14, pad=15)
        plt.xticks(rotation=45, ha='right')
        plt.ylabel('Followers (Millions)', fontsize=12)
        plt.tight_layout()
        
        # Convert to bytes
        img_bytes = BytesIO()
        plt.savefig(img_bytes, format='PNG')
        img_bytes.seek(0)
        plt.close()
        
        return img_bytes
    
    def get_top_artists_data(self):
        """Get top 10 artists by followers with their details"""
        top_artists = self.artist_df.sort_values('spotify_followers', ascending=False).head(10)
        return top_artists[['name', 'spotify_url', 'spotify_image', 'spotify_followers']]