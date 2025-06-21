from dotenv import load_dotenv
from main import fetchVideosFromPlaylist, parse_videos, fuzzy_filter_videos
from sheet import update_video_sheet, _get_current_sheet_data, _setup_google_sheets_connection, print_dataframe_info
from config import YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, MAX_PAGES_TO_FETCH, VIDEO_FILTER, get_games, DEFAULT_PUBLISHED_AFTER_DATE
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

# TODO: Update dashboard as well 
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
        gc, current_sheet = _setup_google_sheets_connection()
        current_df = _get_current_sheet_data(current_sheet)
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

# TODO: Do we need this? find_and_add_game_videos is handles both fetching and adding new videos.
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
    
    # This function would be similar to standard_video_script but focused on a specific game
    # Implementation details would depend on how the game is identified in the playlist
    # For now, we will just call standard_video_script with the same date filter
    video_data = []
    current_page_token = None
    processed_videos_set = set()
    pages_fetched = 0
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
        youtube = googleapiclient.discovery.build(
            YOUTUBE_API_SERVICE_NAME,
            YOUTUBE_API_VERSION,
            developerKey=api_key
        )
    except Exception as e:
        logger.error(f"Error building YouTube client: {e}")
        return []

    keywords = VIDEO_FILTER[game_name]
    logger.info(f"Using keywords for filtering: {keywords}")

    while pages_fetched < MAX_PAGES_TO_FETCH:
        fetch_result = fetchVideosFromPlaylist(
            youtube,
            pageToken=current_page_token,
            proccessed_videos=processed_videos_set,
            published_after_str=published_after_str
        )

        if not fetch_result:
            logger.warning("fetchVideosFromPlaylist returned no result. Stopping.")
            break

        items_on_page = fetch_result.get('items')
        if items_on_page:
            parse_videos(fetch_result, video_data)
        else:
            logger.warning("No items found on this page or an error occurred in fetchVideosFromPlaylist.")

        processed_videos_set = fetch_result.get('processed_videos_set', processed_videos_set)
        current_page_token = fetch_result.get('nextPageToken')
        pages_fetched += 1

        if not current_page_token:
            logger.info("No more pages to fetch.")
            break

        if pages_fetched >= MAX_PAGES_TO_FETCH:
            logger.info("Reached max pages to fetch.")
            break


    if not video_data:
        logger.info("No video data was parsed.")
        return []

    video_data.sort(key=lambda x: x['date'], reverse=True)
    df = pd.DataFrame(video_data)

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
