import os
import googleapiclient.discovery
import pandas as pd
from dotenv import load_dotenv
from main import fetchVideosFromPlaylist, parse_videos, fuzzy_filter_videos
from sheet import update_video_sheet
from config import YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, MAX_PAGES_TO_FETCH

# Load environment variables from .env file
load_dotenv()

def standard_video_script(published_after_date_str: str):
    """
    Fetches YouTube videos from a playlist published after a specific date,
    filters them, and updates a Google Sheet.
    This is a variation of main.testBedMain, focused on requiring a specific date.

    Args:
        published_after_date_str: The date string (YYYY-MM-DD) after which videos should be fetched.
    """
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_key = os.getenv("API_KEY")
    if not api_key:
        print("Error: API_KEY not found. Make sure it's set in your .env file or environment variables.")
        return

    try:
        youtube = googleapiclient.discovery.build(
            YOUTUBE_API_SERVICE_NAME,
            YOUTUBE_API_VERSION,
            developerKey=api_key
        )
    except Exception as e:
        print(f"Error building YouTube client: {e}")
        return

    video_data = []
    current_page_token = None
    processed_videos_set = set()
    pages_fetched = 0

    print(f"Starting video fetch for standard_video_script, for videos published after: {published_after_date_str}")

    while pages_fetched < MAX_PAGES_TO_FETCH:
        print(f"Fetching page {pages_fetched + 1} with token: {current_page_token if current_page_token else 'None'}")
        
        fetch_result = fetchVideosFromPlaylist(
            youtube,
            pageToken=current_page_token,
            proccessed_videos=processed_videos_set,
            published_after_str=published_after_date_str  # Use the function parameter here
        )

        if not fetch_result:
            print("Warning: fetchVideosFromPlaylist returned no result. Stopping.")
            break
        
        items_on_page = fetch_result.get('items')
        if items_on_page:
            parse_videos(fetch_result, video_data)
        else:
            # This can happen if the date filter stops fetching early on a page, or API error.
            # fetchVideosFromPlaylist prints errors for HttpError.
            print("No items found on this page or an error occurred in fetchVideosFromPlaylist.")

        processed_videos_set = fetch_result.get('processed_videos_set', processed_videos_set)
        current_page_token = fetch_result.get('nextPageToken')
        pages_fetched += 1

        if not current_page_token:
            print("No more pages to fetch (end of playlist or date filter met).")
            break

        if pages_fetched >= MAX_PAGES_TO_FETCH:
            print(f"Reached max page fetch limit of {MAX_PAGES_TO_FETCH}.")
            break

    if not video_data:
        print("No videos fetched. Exiting standard_video_script.")
        return

    video_data.sort(key=lambda x: x['date'], reverse=True)
    df = pd.DataFrame(video_data)

    if df.empty:
        print("DataFrame is empty after fetching and parsing. No videos to process.")
        return
    
    print(f"--- \\n Original DF ({len(df)} videos) \\n --- \\n", df.head())
    
    filtered_df = fuzzy_filter_videos(df) 
    
    if filtered_df.empty:
        print("Filtered DataFrame is empty. No videos to update in the sheet.")
        return
        
    print(f"--- \\n Filtered DF ({len(filtered_df)} videos) \\n --- \\n", filtered_df.head())
    
    try:
        update_video_sheet(filtered_df)
        print("standard_video_script completed successfully.")
    except Exception as e:
        print(f"Error during update_video_sheet: {e}")

# Example of how to run this script (optional, for testing):
# if __name__ == "__main__":
#     # This part is for direct execution testing.
#     # Ensure your .env file with API_KEY is accessible.
#     # You might need to run this from the root of your project (rdc-video-bot)
#     # using a command like: python -m rdc_video_bot.script
#     # Or, if rdc_video_bot is in PYTHONPATH: python d:/repos/rdc-video-bot/rdc_video_bot/script.py
#     print("Testing standard_video_script...")
#     # Replace "YYYY-MM-DD" with a valid date for testing
#     standard_video_script("2023-01-01")
