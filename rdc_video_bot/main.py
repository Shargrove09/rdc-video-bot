import os
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from dotenv import load_dotenv
from sheet import update_video_sheet
from datetime import datetime
from rapidfuzz import fuzz, process
from colorama import Fore, Style, init as colorama_init # Import colorama
from config import VIDEO_FILTER, YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, YOUTUBE_PLAYLIST_ID, MAX_PAGES_TO_FETCH, DEFAULT_PUBLISHED_AFTER_DATE


scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

load_dotenv()

def fetchVideosFromPlaylist(youtube, pageToken=None, proccessed_videos=None, published_after_str=None):
    if proccessed_videos is None:
        proccessed_videos = set()

    target_date_obj = None
    if published_after_str:
        try:
            target_date_obj = datetime.strptime(published_after_str, "%Y-%m-%d").date()
        except ValueError:
            print(f"Warning: Invalid date format for published_after_str: '{published_after_str}'. Expected YYYY-MM-DD. Date filter will not be applied.")
            # Keep target_date_obj as None if format is invalid

    if not youtube:
        print("Youtube Client not initialized!")
        return {'items': [], 'nextPageToken': None, 'processed_videos_set': proccessed_videos}

    try:
        playlist_request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            maxResults=50,  # 50 is max limit set by YT API
            playlistId=YOUTUBE_PLAYLIST_ID,  # Using imported constant
            pageToken=pageToken
        )
        playlist_response = playlist_request.execute()
    except googleapiclient.errors.HttpError as e:
        print(f"An API error occurred: {e}")
        return {'items': [], 'nextPageToken': None, 'processed_videos_set': proccessed_videos}

    # print(playlist_response) # Optional: for debugging API response

    fetched_items_on_page = playlist_response.get('items', [])
    # print(f"Fetched {len(fetched_items_on_page)} videos from API for pageToken: {pageToken}") # Optional debug

    filtered_videos_for_return = []
    stop_fetching_more_pages = False

    for item in fetched_items_on_page:
        video_published_at_str = item['contentDetails']['videoPublishedAt']
        # Convert to date object for comparison
        video_published_date = datetime.strptime(video_published_at_str, "%Y-%m-%dT%H:%M:%SZ").date()

        if target_date_obj and video_published_date < target_date_obj:
            # Assuming playlist items are generally ordered newest first.
            # If this item is too old, subsequent items on this page and on future pages are also likely too old.
            print(f"Video '{item['snippet']['title']}' (published {video_published_date}) is older than target date {target_date_obj}. Stopping further pagination.")
            stop_fetching_more_pages = True
            break  # Stop processing items on this page

        video_id = item['contentDetails']['videoId']
        if video_id not in proccessed_videos:
            proccessed_videos.add(video_id)
            filtered_videos_for_return.append(item)

    current_next_page_token = playlist_response.get('nextPageToken')
    
    # If date filter triggered stop, ensure no next page token is returned
    if stop_fetching_more_pages:
        current_next_page_token = None
    
    # print(f"Returning {len(filtered_videos_for_return)} videos after filtering. Next page token: {current_next_page_token}") # Optional debug

    return {
        'items': filtered_videos_for_return,
        'nextPageToken': current_next_page_token,
        'processed_videos_set': proccessed_videos
    }

def parse_videos(playlist_results, video_data_list): 
    # The 'items' key is still present in the dictionary returned by the modified fetchVideosFromPlaylist
    for video in playlist_results.get('items', []): 
        title = video['snippet']['title']
        video_id = video['contentDetails']['videoId']
        print(f"Title: {title}\nVideo ID: {video_id}\n")
        content_details = video['contentDetails']

        date_str = content_details['videoPublishedAt']
        date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        formatted_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")

        video_data_list.append({
            "title": title,
            "video_id": "https://www.youtube.com/watch?v=" + video_id,
            "date": formatted_date,
            "added_to_db": False,
            "date_added_to_db": None
        }) 



def testBedMain(custom_date=None): 
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_key = os.getenv("API_KEY")
    youtube = googleapiclient.discovery.build(
        YOUTUBE_API_SERVICE_NAME, 
        YOUTUBE_API_VERSION,     
        developerKey=api_key)
    
    video_data = []
    current_page_token = None
    processed_videos_set = set()

    # Define target start date
    published_after_filter_date = custom_date if custom_date else DEFAULT_PUBLISHED_AFTER_DATE
    pages_fetched = 0

    while pages_fetched < MAX_PAGES_TO_FETCH: 
        print(f"Fetching page {pages_fetched + 1} for testBedMain with token: {current_page_token}")
        fetch_result = fetchVideosFromPlaylist(youtube,
                                               pageToken=current_page_token,
                                               proccessed_videos=processed_videos_set,
                                               published_after_str=published_after_filter_date)
        
        if not fetch_result:
            print("Error fetching videos for testBedMain. Stopping.")
            break

        if fetch_result.get('items'):
            parse_videos(fetch_result, video_data)
        
        processed_videos_set = fetch_result['processed_videos_set']
        current_page_token = fetch_result.get('nextPageToken')

        pages_fetched += 1
        
        if not current_page_token:
            print("No more pages to fetch for testBedMain (end of playlist or date filter).")
            break

        if pages_fetched >= MAX_PAGES_TO_FETCH:
            print(f"Reached max page fetch limit of {MAX_PAGES_TO_FETCH} for testBedMain.")
            break
    
    video_data.sort(key=lambda x: x['date'], reverse=True)
    df = pd.DataFrame(video_data)
    filtered_df = fuzzy_filter_videos(df)
    print("--- \n Filtered DF \n --- \n", filtered_df)
    update_video_sheet(filtered_df)

def fuzzy_filter_videos(videos, threshold=80):
    filtered_videos = []
    
    for _, video in videos.iterrows():
        title = video['title'].lower()
        matched_games = set()
        
        for game, keywords in VIDEO_FILTER.items():
            for keyword in keywords:
                score = fuzz.partial_ratio(keyword.lower(), title)
                if score > threshold:
                    matched_games.add(game)
        print(f'Matched games for {title}: {matched_games}')
        
        if matched_games:
            video = video.copy()  # Avoid SettingWithCopyWarning
            video['games'] = ', '.join(sorted(matched_games))
            filtered_videos.append(video)
    
    return pd.DataFrame(filtered_videos)


def interactive_menu():
    """Displays an interactive menu to the user."""
    colorama_init(autoreset=True)  # Initialize colorama
    while True:
        print(f"\n{Fore.CYAN}--- RDC Video Bot Menu ---{Style.RESET_ALL}")
        print(f"{Fore.GREEN}1. Fetch and update videos (current default behavior){Style.RESET_ALL}")
        print(f"{Fore.YELLOW}2. Fetch stats from dashboard (Not Implemented Yet){Style.RESET_ALL}")
        print(f"{Fore.GREEN}3. Fetch videos from a specific date{Style.RESET_ALL}")
        print(f"{Fore.RED}4. Exit{Style.RESET_ALL}")

        choice = input(f"{Fore.BLUE}Enter your choice (1-4): {Style.RESET_ALL}")

        if choice == '1':
            print(f"{Fore.GREEN}Running: Fetch and update videos...{Style.RESET_ALL}")
            testBedMain()
        elif choice == '2':
            print(f"{Fore.YELLOW}Fetching stats from dashboard... (Not Implemented Yet){Style.RESET_ALL}")
            # Placeholder for fetch_dashboard_stats()
        elif choice == '3':
            date_input = input(f"{Fore.BLUE}Enter the date to fetch videos from (YYYY-MM-DD): {Style.RESET_ALL}")
            try:
                # Validate the date format
                datetime.strptime(date_input, "%Y-%m-%d")
                print(f"{Fore.GREEN}Fetching videos from {date_input}...{Style.RESET_ALL}")
                testBedMain(custom_date=date_input)
            except ValueError:
                print(f"{Fore.RED}Invalid date format. Please use YYYY-MM-DD format (e.g. 2025-06-10){Style.RESET_ALL}")
        elif choice == '4':
            print(f"{Fore.RED}Exiting.{Style.RESET_ALL}")
            break
        else:
            print(f"{Fore.RED}Invalid choice. Please try again.{Style.RESET_ALL}")

if __name__ == "__main__":
    interactive_menu()