import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
import pandas as pd
from datetime import datetime
import requests
from io import BytesIO
from PIL import Image

from config import TELEGRAM_TOKEN, DEFAULT_ARTISTS_COUNT, DEFAULT_GENRE, DATA_DIR
from data_collector import ArtistDataCollector
from visualizer import ArtistVisualizer

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Store user data
user_data = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    await update.message.reply_text(
        f"Hello, {user.first_name}! 👋\n\n"
        f"I'm the Music Artist Analytics Bot. I can fetch and analyze music artists data by genre.\n\n"
        f"Use /analyze to start a new analysis.\n"
        f"Use /help to see all available commands."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""
    await update.message.reply_text(
        "Here's what I can do:\n\n"
        "/start - Start the bot\n"
        "/help - Show this help message\n"
        "/analyze - Begin a new analysis\n"
        "/end - End the conversation\n\n"
        "When you start an analysis, I'll ask you for:\n"
        "1. A music genre or tag (e.g., 'Metal', 'Jazz', 'Hip Hop')\n"
        "2. How many artists to analyze (max 1000)\n\n"
        "I'll then fetch data from MusicBrainz and Spotify, process it, and show you insights!"
    )

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start the analysis process by asking for a genre."""
    await update.message.reply_text(
        "Let's analyze some music artists! 🎵\n\n"
        "First, tell me which genre or tag you're interested in:\n"
        "(e.g., 'Metal', 'Jazz', 'Rock', 'Pop', 'Hip Hop', etc.)"
    )
    # Set the next step
    user_data[update.effective_user.id] = {"state": "waiting_for_genre"}

async def end(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """End the conversation."""
    if update.effective_user.id in user_data:
        del user_data[update.effective_user.id]
    await update.message.reply_text(
        "Thanks for using the Music Artist Analytics Bot! 👋\n"
        "Come back anytime for more insights on your favorite music genres."
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle user messages based on conversation state."""
    user_id = update.effective_user.id
    
    # Update last activity time
    if user_id in user_data:
        user_data[user_id]["last_activity"] = datetime.now()
    
    # If user not in conversation, ignore
    if user_id not in user_data:
        await update.message.reply_text(
            "Use /analyze to start a new analysis, or /help to see available commands."
        )
        return
    
    state = user_data[user_id].get("state")
    
    # Add a safety check for state
    if not state:
        await update.message.reply_text(
            "Something went wrong. Let's start over. Use /analyze to begin."
        )
        return
    
    if state == "waiting_for_genre":
        # Process genre input
        genre = update.message.text.strip()
        if not genre:
            await update.message.reply_text("Please enter a valid genre.")
            return
        
        user_data[user_id]["genre"] = genre
        user_data[user_id]["state"] = "waiting_for_count"
        
        await update.message.reply_text(
            f"Great! Now tell me how many artists to analyze (10-1000):\n"
            f"(Default is {DEFAULT_ARTISTS_COUNT} if you just press Enter)"
        )
    
    elif state == "waiting_for_count":
        # Process count input
        count_text = update.message.text.strip()
        
        if not count_text:
            # Use default
            count = DEFAULT_ARTISTS_COUNT
        else:
            try:
                count = int(count_text)
                if count < 10:
                    await update.message.reply_text("Please enter a number at least 10.")
                    return
                if count > 1000:
                    await update.message.reply_text("Please enter a number no more than 1000.")
                    return
            except ValueError:
                await update.message.reply_text("Please enter a valid number.")
                return
        
        user_data[user_id]["count"] = count
        user_data[user_id]["state"] = "collecting"
        
        # Start the data collection process
        genre = user_data[user_id]["genre"]
        await update.message.reply_text(
            f"Starting analysis of {count} {genre} artists! 🎸\n\n"
            f"This may take a few minutes, please be patient while I gather the data..."
        )
        
        # Run data collection in background
        context.application.create_task(
            process_data(update, context, genre, count)
        )

async def send_progress(update: Update, context: ContextTypes.DEFAULT_TYPE, message: str) -> None:
    """Send progress updates to the user."""
    user_id = update.effective_user.id
    
    # Initialize user data structure if not existing
    if "current_phase" not in user_data.get(user_id, {}):
        # Ensure user_id key exists
        if user_id not in user_data:
            user_data[user_id] = {}
            
        user_data[user_id]["current_phase"] = "Fetching"
    
    # Parse message to determine phase
    if "Fetching" in message:
        user_data[user_id]["current_phase"] = "Fetching Artists"
    elif "Processing" in message:
        user_data[user_id]["current_phase"] = "Cleaning Data"
    elif "country" in message.lower():
        user_data[user_id]["current_phase"] = "Geographic Analysis"
    elif "Spotify" in message:
        user_data[user_id]["current_phase"] = "Adding Streaming Data"
    elif "saved" in message:
        user_data[user_id]["current_phase"] = "Completing Analysis"
    
    # Create a simplified progress message
    progress_text = f"🔍 Analyzing {user_data[user_id].get('genre', 'music')} artists:\n\n"
    
    # Add current phase info
    progress_text += f"Current phase: {user_data[user_id]['current_phase']}\n"
    progress_text += f"Details: {message}"
    
    # Send or update message
    if not user_data[user_id].get("progress_message_id"):
        msg = await update.message.reply_text(progress_text)
        user_data[user_id]["progress_message_id"] = msg.message_id
    else:
        try:
            await context.bot.edit_message_text(
                progress_text,
                chat_id=update.effective_chat.id,
                message_id=user_data[user_id]["progress_message_id"]
            )
        except Exception as e:
            logger.error(f"Failed to update progress message: {e}")

async def process_data(update: Update, context: ContextTypes.DEFAULT_TYPE, genre: str, count: int) -> None:
    """Process data and show results."""
    user_id = update.effective_user.id
    
    # Create data collector with progress callback
    collector = ArtistDataCollector(genre, count)
    
    async def progress_callback(message):
        await send_progress(update, context, message)
    
    # Set up progress callback
    collector.set_progress_callback(lambda msg: context.application.create_task(progress_callback(msg)))
    
    try:
        # Run data collection
        artist_df, countries_df = collector.collect_all_data()
        
        if artist_df is None or len(artist_df) == 0:
            await update.message.reply_text(
                f"Sorry, I couldn't find any {genre} artists. Try another genre?"
            )
            user_data[user_id]["state"] = "idle"
            # Clean up any resources
            if "progress_message_id" in user_data[user_id]:
                del user_data[user_id]["progress_message_id"]
            return
        
        # Create visualizer
        visualizer = ArtistVisualizer(artist_df, countries_df, genre)
        
        # Send summary
        summary = visualizer.create_summary_text()
        await update.message.reply_text(summary)
        
        # Send top artists with images
        await send_top_artists(update, context, visualizer)
        
        # Show plot selection menu
        await show_plot_menu(update, context)
        
        # Update user state
        user_data[user_id]["state"] = "showing_results"
        user_data[user_id]["visualizer"] = visualizer
        
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        await update.message.reply_text(
            f"Sorry, an error occurred while processing the data: {str(e)}\n"
            f"Please try again with a different genre or count."
        )
        user_data[user_id]["state"] = "idle"
        # Clean up any resources
        if "progress_message_id" in user_data[user_id]:
            del user_data[user_id]["progress_message_id"]

async def get_and_send_image(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str) -> None:
    """Fetch, resize and send an image to the user"""
    try:
        response = requests.get(url)
        if response.status_code == 200:
            # Process image
            image = Image.open(BytesIO(response.content))
            
            # Calculate new height maintaining aspect ratio
            max_width = 300
            width_percent = max_width / float(image.size[0])
            new_height = int(float(image.size[1]) * width_percent)
            
            # Resize image
            image = image.resize((max_width, new_height), Image.Resampling.LANCZOS)
            
            # Convert to bytes
            img_byte_arr = BytesIO()
            image.save(img_byte_arr, format='PNG')
            img_byte_arr.seek(0)
            
            # Send image
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=img_byte_arr
            )
            return True
        return None
    except Exception as e:
        logger.error(f"Error sending image: {e}")
        return None

async def send_top_artists(update: Update, context: ContextTypes.DEFAULT_TYPE, visualizer: ArtistVisualizer) -> None:
    """Send top artists with their images and Spotify URLs."""
    top_artists = visualizer.get_top_artists_data()
    
    await update.message.reply_text(f"Top {len(top_artists)} Artists with Spotify URLs and Images:")
    
    for i, (idx, row) in enumerate(top_artists.iterrows(), 1):
        # Send artist info
        message = f"{i}. {row['name']}\n"
        message += f"🎵 Listen: {row['spotify_url']}\n"
        message += f"👥 Followers: {int(row['spotify_followers']):,}"
        
        await update.message.reply_text(message)
        
        # Send artist image if available
        if pd.notna(row['spotify_image']):
            await get_and_send_image(update, context, row['spotify_image'])

async def show_plot_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show menu for selecting plots."""
    keyboard = [
        [
            InlineKeyboardButton("1. Top by Popularity", callback_data="plot_popularity"),
            InlineKeyboardButton("2. Top by Followers", callback_data="plot_followers"),
        ],
        [
            InlineKeyboardButton("3. Formation Year Distribution", callback_data="plot_years"),
            InlineKeyboardButton("4. Active Artists Map", callback_data="plot_map"),
        ],
        [
            InlineKeyboardButton("5. Artists per Million", callback_data="plot_per_million"),
            InlineKeyboardButton("6. All Plots", callback_data="plot_all"),
        ],
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Choose a plot to view:", reply_markup=reply_markup)

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks for plot selection."""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    if user_id not in user_data or "visualizer" not in user_data[user_id]:
        await query.edit_message_text("Session expired. Use /analyze to start a new analysis.")
        return
    
    visualizer = user_data[user_id]["visualizer"]
    
    await query.edit_message_text("Generating plot, please wait...")
    
    try:
        if query.data == "plot_popularity":
            # Popularity plot
            img_bytes = visualizer.plot_top_artists_popularity()
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=img_bytes,
                caption="Top 10 Artists by Spotify Popularity"
            )
        
        elif query.data == "plot_followers":
            # Followers plot
            img_bytes = visualizer.plot_top_artists_followers()
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=img_bytes,
                caption="Top 10 Artists by Spotify Followers"
            )
        
        elif query.data == "plot_years":
            # Years distribution plot
            img_bytes = visualizer.plot_year_distribution()
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=img_bytes,
                caption="Distribution of Artist Formation Years"
            )
        
        elif query.data == "plot_map":
            # Map of active artists
            img_bytes = visualizer.plot_active_artists_map()
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=img_bytes,
                caption="Active Artists by Country"
            )
        
        elif query.data == "plot_per_million":
            # Artists per million plot
            img_bytes = visualizer.plot_artists_per_million()
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=img_bytes,
                caption="Artists per Million People by Country"
            )
        
        elif query.data == "plot_all":
            # Send all plots
            await query.edit_message_text("Generating all plots, please wait...")
            
            # Popularity plot
            img_bytes = visualizer.plot_top_artists_popularity()
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=img_bytes,
                caption="Top 10 Artists by Spotify Popularity"
            )
            
            # Followers plot
            img_bytes = visualizer.plot_top_artists_followers()
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=img_bytes,
                caption="Top 10 Artists by Spotify Followers"
            )
            
            # Years distribution plot
            img_bytes = visualizer.plot_year_distribution()
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=img_bytes,
                caption="Distribution of Artist Formation Years"
            )
            
            # Map of active artists
            img_bytes = visualizer.plot_active_artists_map()
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=img_bytes,
                caption="Active Artists by Country"
            )
            
            # Artists per million plot
            img_bytes = visualizer.plot_artists_per_million()
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=img_bytes,
                caption="Artists per Million People by Country"
            )
            
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="All plots generated successfully!"
            )
            return
        
        # Show the menu again after sending a plot
        keyboard = [
            [
                InlineKeyboardButton("1. Top by Popularity", callback_data="plot_popularity"),
                InlineKeyboardButton("2. Top by Followers", callback_data="plot_followers"),
            ],
            [
                InlineKeyboardButton("3. Formation Year Distribution", callback_data="plot_years"),
                InlineKeyboardButton("4. Active Artists Map", callback_data="plot_map"),
            ],
            [
                InlineKeyboardButton("5. Artists per Million", callback_data="plot_per_million"),
                InlineKeyboardButton("6. All Plots", callback_data="plot_all"),
            ],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Choose another plot to view:",
            reply_markup=reply_markup
        )
    
    except Exception as e:
        logger.error(f"Error generating plot: {e}")
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"Sorry, an error occurred while generating the plot: {str(e)}"
        )

async def cleanup_old_sessions(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove old user sessions"""
    current_time = datetime.now()
    users_to_remove = []
    
    for user_id, data in user_data.items():
        # If last activity was more than 30 minutes ago
        if "last_activity" in data:
            if (current_time - data["last_activity"]).total_seconds() > 1800:  # 30 minutes
                users_to_remove.append(user_id)
    
    # Remove expired sessions
    for user_id in users_to_remove:
        del user_data[user_id]
        logger.info(f"Removed expired session for user {user_id}")

def main() -> None:
    """Start the bot."""
    # Create the Application
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Add conversation handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("analyze", analyze))
    application.add_handler(CommandHandler("end", end))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(button_callback))

    # Add job to clean up old sessions every 10 minutes
    job_queue = application.job_queue
    job_queue.run_repeating(cleanup_old_sessions, interval=600)

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()