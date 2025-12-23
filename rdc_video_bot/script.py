from dotenv import load_dotenv
from main import fetch_and_process_videos, parse_video_data, fuzzy_filter_videos, YouTubeClient
from sheet import update_video_sheet, GoogleSheetsClient
from config import YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, MAX_PAGES_TO_FETCH, VIDEO_FILTER, get_games, DEFAULT_PUBLISHED_AFTER_DATE, SPREADSHEET_NAME
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
import pandas as pd
import os
import logging
import sys
import googleapiclient.discovery


# Load environment variables from .env file
load_dotenv()

def setup_logging(log_dir="logs"):
    """Configure logging to both console and file with rotation."""
    # Create logs directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    # Generate log filename with date
    log_filename = os.path.join(log_dir, f"video_bot_{datetime.now().strftime('%Y-%m-%d')}.log")
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers (important for repeated runs)
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Create file handler for logging to file
    file_handler = RotatingFileHandler(
        log_filename, maxBytes=10*1024*1024, backupCount=5
    )
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    # Create console handler for logging to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(file_format)
    
    # Add both handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logging.info(f"Logging initialized. Log file: {log_filename}")
    return logger

def print_dataframe_info(df: pd.DataFrame, title: str = "DataFrame Info"):
    """Helper function to print basic dataframe information."""
    if df is None or df.empty:
        print(f"\n{title}: DataFrame is empty or None")
        return
    print(f"\n{title}:")
    print(f"  Total rows: {len(df)}")
    if 'video_id' in df.columns:
        print(f"  Unique video IDs: {df['video_id'].nunique()}")
    if 'date' in df.columns:
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")

# TODO: Update dashboard as well 
# TODO: This is safe to delete? Check after fixing cronjob
def standard_video_script(published_after_date_str: str):
    """
    Fetches YouTube videos from a playlist published after a specific date,
    filters them, and updates a Google Sheet.

    Args:
        published_after_date_str: The date string (YYYY-MM-DD) after which videos should be fetched.
    """
    logger = setup_logging()
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_key = os.getenv("API_KEY")
    if not api_key:
        logger.error("API_KEY not found. Make sure it's set in your .env file or environment variables.")
        return

    try:
        youtube = YouTubeClient(api_key)
    except Exception as e:
        logger.error(f"Error building YouTube client: {e}")
        return

    logger.info(f"Starting video fetch for standard_video_script, for videos published after: {published_after_date_str}")

    # fetch_and_process_videos now handles pagination internally and returns a list of raw video items
    raw_videos = fetch_and_process_videos(
        youtube,
        published_after_str=published_after_date_str
    )

    if not raw_videos:
        logger.info("No videos fetched. Exiting standard_video_script.")
        return

    # Parse the raw video data into a DataFrame
    df = parse_video_data(raw_videos)

    if df.empty:
        logger.info("DataFrame is empty after fetching and parsing. No videos to process.")
        return
    
    logger.info(f"--- \n Original DF ({len(df)} videos) \n --- \n {df.head()}")
    
    filtered_df = fuzzy_filter_videos(df) 
    
    if filtered_df.empty:
        logger.info("Filtered DataFrame is empty. No videos to update in the sheet.")
        return
        
    logger.info(f"--- \n Filtered DF ({len(filtered_df)} videos) \n --- \n {filtered_df.head()}")
    
    try:
        update_video_sheet(filtered_df)
        logger.info("standard_video_script completed successfully.")
    except Exception as e:
        logger.error(f"Error during update_video_sheet: {e}")

def find_and_add_game_videos(game_name, published_after_str=DEFAULT_PUBLISHED_AFTER_DATE):
    """
    Finds videos for a specific game that are not already in the sheet
    and adds them after user confirmation.
    
    Args:
        game_name: The name of the game to filter videos by.
        published_after_str: The date string (YYYY-MM-DD) after which videos should be fetched.
    """
    logger = setup_logging()
    
    # Prompt user for date input
    user_date = input(f"Enter the date to search from (YYYY-MM-DD) or press Enter to use default ({published_after_str}): ").strip()
    if user_date:
        try:
            # Validate date format
            datetime.strptime(user_date, "%Y-%m-%d")
            published_after_str = user_date
        except ValueError:
            logger.warning(f"Invalid date format '{user_date}'. Using default date: {published_after_str}")
    
    logger.info(f"Starting to find and add videos for game: {game_name} published after {published_after_str}")

    fetched_videos_list = fetch_game_videos_from_playlist(game_name, published_after_str)
    if not fetched_videos_list:
        logger.info(f"No videos found for game '{game_name}' published after {published_after_str}. Exiting.")
        return

    fetched_df = pd.DataFrame(fetched_videos_list)

    try:
        client = GoogleSheetsClient(SPREADSHEET_NAME)
        current_df = client.get_sheet_as_dataframe(client.main_sheet.title)
        if current_df is None:
            current_df = pd.DataFrame()
    except Exception as e:
        logger.error(f"Error connecting to Google Sheets: {e}")
        return

    if not current_df.empty and 'video_id' in current_df.columns:
        existing_video_ids = set(current_df['video_id'])
        new_videos_df = fetched_df[~fetched_df['video_id'].isin(existing_video_ids)]
    else:
        new_videos_df = fetched_df

    if new_videos_df.empty:
        logger.info(f"No new videos for game '{game_name}' to add to the sheet.")
        return

    logger.info(f"Found {len(new_videos_df)} new videos for '{game_name}' not present in the sheet.")
    
    # Reuse existing helper to show dataframe info
    print_dataframe_info(new_videos_df, f"New Videos for '{game_name}'")
    see_videos = input("Would you like to see the new videos? (y/n): ").strip().lower()
    if see_videos == 'y':
        for idx, row in new_videos_df.iterrows():
            print(f"\n[{idx+1}] Title: {row.get('title', '')}")
            print(f"    Video ID: {row.get('video_id', '')}")
            print(f"    Date: {row.get('date', '')}")
    user_input = input("Would you like to add these videos to the sheet? (y/n): ").strip().lower()
    if user_input == 'y':
        try:
            update_video_sheet(new_videos_df)
            logger.info(f"Successfully added {len(new_videos_df)} new videos for '{game_name}' to the sheet.")
        except Exception as e:
            logger.error(f"Error updating sheet with new videos: {e}")
    else:
        logger.info("User chose not to add the new videos. Exiting.")

def fetch_game_videos_from_playlist(game_name, published_after_str=DEFAULT_PUBLISHED_AFTER_DATE):
    """
    Fetches videos from a YouTube playlist for a specific game.
    
    Args:
        game_name: The name of the game to filter videos by.
        published_after_str: The date string (YYYY-MM-DD) after which videos should be fetched.
    
    Returns:
        A list of video data dictionaries.
    """
    logger = setup_logging()
    logger.info(f"Fetching videos for game: {game_name} published after {published_after_str}")
    
    api_key = os.getenv("API_KEY")

    if game_name not in VIDEO_FILTER:
        logger.error(f"Game '{game_name}' not found in VIDEO_FILTER. Available games: {list(VIDEO_FILTER.keys())}")
        return []
    
     
    # Prompt user for date input
    user_date = input(f"Enter the date to search from (YYYY-MM-DD) or press Enter to use default ({published_after_str}): ").strip()
    if user_date:
        try:
            # Validate date format
            datetime.strptime(user_date, "%Y-%m-%d")
            published_after_str = user_date
        except ValueError:
            logger.warning(f"Invalid date format '{user_date}'. Using default date: {published_after_str}")
   
    try:
        youtube = YouTubeClient(api_key)
    except Exception as e:
        logger.error(f"Error building YouTube client: {e}")
        return []

    keywords = VIDEO_FILTER[game_name]
    logger.info(f"Using keywords for filtering: {keywords}")

    # fetch_and_process_videos now handles pagination internally and returns a list of raw video items
    raw_videos = fetch_and_process_videos(
        youtube,
        published_after_str=published_after_str
    )

    if not raw_videos:
        logger.info("No video data was parsed.")
        return []

    # Parse the raw video data into a DataFrame
    df = parse_video_data(raw_videos)

    if df.empty:
        logger.info("DataFrame is empty after fetching and parsing. No videos to process.")
        return []

    # Filter videos by keywords for the game
    def matches_keywords(row):
        title = row['title'].lower()
        return any(keyword.lower() in title for keyword in keywords)

    filtered_df = df[df.apply(matches_keywords, axis=1)]

    if filtered_df.empty:
        logger.info(f"No videos found for game '{game_name}' matching the keywords.")
        return []

    logger.info(f"Found {len(filtered_df)} videos for game '{game_name}'.")
    
    return filtered_df.to_dict(orient='records')


# Example of how to run this script (optional, for testing):
if __name__ == "__main__":
    logger = setup_logging() # Setup logging for direct script run as well
    import sys
    if len(sys.argv) > 1:
        date_param = sys.argv[1]
    else:
        date_param = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Testing standard_video_script with date: {date_param}")
    standard_video_script(date_param)
