import os
import googleapiclient.discovery
import pandas as pd
from dotenv import load_dotenv
from main import fetchVideosFromPlaylist, parse_videos, fuzzy_filter_videos
from sheet import update_video_sheet
from config import YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, MAX_PAGES_TO_FETCH
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler


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

def standard_video_script(published_after_date_str: str):
    """
    Fetches YouTube videos from a playlist published after a specific date,
    filters them, and updates a Google Sheet.
    This is a variation of main.testBedMain, focused on requiring a specific date.

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
        youtube = googleapiclient.discovery.build(
            YOUTUBE_API_SERVICE_NAME,
            YOUTUBE_API_VERSION,
            developerKey=api_key
        )
    except Exception as e:
        logger.error(f"Error building YouTube client: {e}")
        return

    video_data = []
    current_page_token = None
    processed_videos_set = set()
    pages_fetched = 0

    logger.info(f"Starting video fetch for standard_video_script, for videos published after: {published_after_date_str}")

    while pages_fetched < MAX_PAGES_TO_FETCH:
        logger.info(f"Fetching page {pages_fetched + 1} with token: {current_page_token if current_page_token else 'None'}")
        
        fetch_result = fetchVideosFromPlaylist(
            youtube,
            pageToken=current_page_token,
            proccessed_videos=processed_videos_set,
            published_after_str=published_after_date_str  # Use the function parameter here
        )

        if not fetch_result:
            logger.warning("fetchVideosFromPlaylist returned no result. Stopping.")
            break
        
        items_on_page = fetch_result.get('items')
        if items_on_page:
            parse_videos(fetch_result, video_data)
        else:
            # This can happen if the date filter stops fetching early on a page, or API error.
            # fetchVideosFromPlaylist prints errors for HttpError.
            logger.warning("No items found on this page or an error occurred in fetchVideosFromPlaylist.")

        processed_videos_set = fetch_result.get('processed_videos_set', processed_videos_set)
        current_page_token = fetch_result.get('nextPageToken')
        pages_fetched += 1

        if not current_page_token:
            logger.info("No more pages to fetch (end of playlist or date filter met).")
            break

        if pages_fetched >= MAX_PAGES_TO_FETCH:
            logger.info(f"Reached max page fetch limit of {MAX_PAGES_TO_FETCH}.")
            break

    if not video_data:
        logger.info("No videos fetched. Exiting standard_video_script.")
        return

    video_data.sort(key=lambda x: x['date'], reverse=True)
    df = pd.DataFrame(video_data)

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
