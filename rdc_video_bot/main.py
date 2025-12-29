"""
This module is the main entry point for the RDC Video Bot.
It provides an interactive menu to fetch video data from a YouTube playlist,
filter it, and update a Google Sheet.
"""
import os
from datetime import datetime
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from dotenv import load_dotenv
from rapidfuzz import fuzz
from colorama import Fore, Style, init as colorama_init
from typing import List, Dict, Set, Optional, Tuple, Any

from sheet import update_video_sheet, fetch_dashboard_stats, fetch_latest_videos
from config import (
    VIDEO_FILTER, YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    YOUTUBE_PLAYLIST_ID, MAX_PAGES_TO_FETCH, DEFAULT_PUBLISHED_AFTER_DATE,
    get_games, VIDEO_COLUMNS
)

# --- YouTube Client Class ---

class YouTubeClient:
    """A client to interact with the YouTube Data API."""

    def __init__(self, api_key: str):
        """
        Initializes the YouTube API client.
        Args:
            api_key: The YouTube Data API key.
        """
        if not api_key:
            raise ValueError("API key cannot be empty.")
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
        self.youtube = googleapiclient.discovery.build(
            YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=api_key
        )

    def fetch_playlist_page(self, page_token: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Fetches a single page of playlist items.
        Args:
            page_token: The token for the next page of results.
        Returns:
            The API response dictionary or None if an error occurs.
        """
        try:
            request = self.youtube.playlistItems().list(
                part="snippet,contentDetails",
                maxResults=50,
                playlistId=YOUTUBE_PLAYLIST_ID,
                pageToken=page_token
            )
            return request.execute()
        except googleapiclient.errors.HttpError as e:
            print(f"{Fore.RED}An API error occurred: {e}{Style.RESET_ALL}")
            return None

# --- Core Logic Functions ---

def fetch_and_process_videos(youtube_client: YouTubeClient, published_after_str: str) -> List[Dict[str, Any]]:
    """
    Fetches and processes all videos from a playlist from a given date.
    Args:
        youtube_client: An instance of the YouTubeClient.
        published_after_str: The earliest publish date for videos (YYYY-MM-DD).
    Returns:
        A list of dictionaries, where each dictionary represents a processed video.
    """
    all_videos = []
    processed_video_ids: Set[str] = set()
    current_page_token: Optional[str] = None
    pages_fetched = 0

    try:
        target_date = datetime.strptime(published_after_str, "%Y-%m-%d").date()
    except ValueError:
        print(f"{Fore.RED}Invalid date format: '{published_after_str}'. Using default.{Style.RESET_ALL}")
        target_date = datetime.strptime(DEFAULT_PUBLISHED_AFTER_DATE, "%Y-%m-%d").date()

    while pages_fetched < MAX_PAGES_TO_FETCH:
        print(f"Fetching page {pages_fetched + 1}...")
        response = youtube_client.fetch_playlist_page(current_page_token)

        if not response:
            break

        items = response.get('items', [])
        if not items:
            print("No items found on this page.")
            break

        stop_fetching = False
        for item in items:
            video_id = item['contentDetails']['videoId']
            published_at_str = item['contentDetails']['videoPublishedAt']
            published_date = datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ").date()

            if published_date < target_date:
                stop_fetching = True
                break

            if video_id not in processed_video_ids:
                processed_video_ids.add(video_id)
                all_videos.append(item)

        if stop_fetching:
            print(f"Reached videos older than {target_date}. Stopping.")
            break

        current_page_token = response.get('nextPageToken')
        if not current_page_token:
            print("End of playlist reached.")
            break

        pages_fetched += 1
        if pages_fetched >= MAX_PAGES_TO_FETCH:
            print(f"Reached max page fetch limit of {MAX_PAGES_TO_FETCH}.")

    return all_videos

def parse_video_data(videos: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Parses raw video data from the API into a structured DataFrame.
    Args:
        videos: A list of video items from the YouTube API.
    Returns:
        A DataFrame containing structured video data.
    """
    video_data_list = []
    for video in videos:
        title = video['snippet']['title']
        video_id = video['contentDetails']['videoId']
        date_str = video['contentDetails']['videoPublishedAt']
        date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")

        # Initialize all columns from centralized schema with None defaults
        video_dict = {col: None for col in VIDEO_COLUMNS}
        # Populate known values
        video_dict.update({
            "title": title,
            "video_id": f"https://www.youtube.com/watch?v={video_id}",
            "date": date_obj.strftime("%Y-%m-%d %H:%M:%S"),
            "added_to_db": False,
            "has_screenshots": False
        })
        video_data_list.append(video_dict)
    return pd.DataFrame(video_data_list)

def fuzzy_filter_videos(videos_df: pd.DataFrame, threshold: int = 80) -> pd.DataFrame:
    """
    Filters a DataFrame of videos based on fuzzy matching of titles.
    This version is optimized for performance.
    Args:
        videos_df: DataFrame with video data, including a 'title' column.
        threshold: Fuzzy match confidence score (0-100).
    Returns:
        A new DataFrame with only the matched videos and a 'games' column.
    """
    if videos_df.empty:
        return pd.DataFrame()

    # Pre-compile keywords for faster access
    game_keywords = {game: [kw.lower() for kw in keywords] for game, keywords in VIDEO_FILTER.items()}
    
    filtered_data = []
    for video_row in videos_df.itertuples(index=False):
        title_lower = video_row.title.lower()
        matched_games: Set[str] = set()

        for game, keywords in game_keywords.items():
            for keyword in keywords:
                if fuzz.partial_ratio(keyword, title_lower) > threshold:
                    matched_games.add(game)
                    # Break after first keyword match for a game to be slightly faster
                    break 
        
        if matched_games:
            # Convert NamedTuple to dict and add games
            video_dict = video_row._asdict()
            video_dict['games'] = ', '.join(sorted(matched_games))
            filtered_data.append(video_dict)
            
    return pd.DataFrame(filtered_data)

# --- UI and Main Execution Functions ---

def display_dashboard_stats():
    """Fetches and displays dashboard statistics in a formatted way."""
    dashboard_df = fetch_dashboard_stats()
    
    if dashboard_df is None:
        print(f"{Fore.RED}Failed to fetch dashboard statistics. Try running option 1 first.{Style.RESET_ALL}")
        return
    
    if dashboard_df.empty:
        print(f"{Fore.YELLOW}Dashboard sheet is empty.{Style.RESET_ALL}")
        return
    
    print(f"\n{Fore.CYAN}=== Dashboard Statistics ==={Style.RESET_ALL}")
    
    pd.set_option('display.width', 1000)
    
    if 'Statistic' in dashboard_df.columns and 'Value' in dashboard_df.columns:
        for _, row in dashboard_df.iterrows():
            stat, val = row['Statistic'], row['Value']
            color = Fore.GREEN
            if stat.startswith('---'): color = Fore.CYAN
            elif 'not' in str(stat).lower() or 'error' in str(stat).lower(): color = Fore.YELLOW
            print(f"{color}{stat}: {val}{Style.RESET_ALL}")
    else:
        print(dashboard_df.to_string())
    
    pd.reset_option('display.width')

def display_latest_videos():
    """Fetches and displays the latest videos from the sheet."""
    try:
        limit_input = input(f"{Fore.BLUE}How many rows to show? (Default 10): {Style.RESET_ALL}")
        limit = int(limit_input) if limit_input.strip() else 10
    except ValueError:
        print(f"{Fore.RED}Invalid input. Using default of 10.{Style.RESET_ALL}")
        limit = 10

    print(f"Fetching latest {limit} videos...")
    latest_videos = fetch_latest_videos(limit)

    if latest_videos is not None and not latest_videos.empty:
        print(f"\n{Fore.CYAN}=== Latest {limit} Videos ==={Style.RESET_ALL}")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(latest_videos.to_string(index=False))
        pd.reset_option('display.max_columns')
        pd.reset_option('display.width')
    else:
        print(f"{Fore.YELLOW}No videos found.{Style.RESET_ALL}")

def search_game_videos_from_playlist(youtube_client: YouTubeClient):
    """Searches and displays videos for a specific game from the YouTube playlist."""
    games = get_games()
    
    if not games:
        print(f"{Fore.RED}No games configured in video filter.{Style.RESET_ALL}")
        return
    
    # Display available games
    print(f"\n{Fore.CYAN}=== Available Games ==={Style.RESET_ALL}")
    for idx, game in enumerate(games, 1):
        print(f"{idx}. {game}")
    
    # Get user selection
    try:
        choice = input(f"{Fore.BLUE}Select a game (1-{len(games)}): {Style.RESET_ALL}")
        choice_idx = int(choice) - 1
        if choice_idx < 0 or choice_idx >= len(games):
            print(f"{Fore.RED}Invalid selection.{Style.RESET_ALL}")
            return
        selected_game = games[choice_idx]
    except (ValueError, IndexError):
        print(f"{Fore.RED}Invalid input.{Style.RESET_ALL}")
        return
    
    # Get optional date input
    date_input = input(f"{Fore.BLUE}Enter start date (YYYY-MM-DD) or press Enter for default ({DEFAULT_PUBLISHED_AFTER_DATE}): {Style.RESET_ALL}").strip()
    
    if date_input:
        try:
            datetime.strptime(date_input, "%Y-%m-%d")
            published_after_date = date_input
        except ValueError:
            print(f"{Fore.RED}Invalid date format. Using default: {DEFAULT_PUBLISHED_AFTER_DATE}{Style.RESET_ALL}")
            published_after_date = DEFAULT_PUBLISHED_AFTER_DATE
    else:
        published_after_date = DEFAULT_PUBLISHED_AFTER_DATE
    
    print(f"\nSearching for {Fore.CYAN}{selected_game}{Style.RESET_ALL} videos published after {published_after_date}...")
    
    # Fetch videos from YouTube
    raw_videos = fetch_and_process_videos(youtube_client, published_after_date)
    
    if not raw_videos:
        print(f"{Fore.YELLOW}No videos found.{Style.RESET_ALL}")
        return
    
    # Parse video data
    video_df = parse_video_data(raw_videos)
    
    if video_df.empty:
        print(f"{Fore.YELLOW}No videos to process.{Style.RESET_ALL}")
        return
    
    # Filter by selected game using fuzzy matching
    keywords = VIDEO_FILTER.get(selected_game, [])
    if not keywords:
        print(f"{Fore.RED}No keywords configured for {selected_game}.{Style.RESET_ALL}")
        return
    
    keywords_lower = [kw.lower() for kw in keywords]
    threshold = 80
    
    def matches_game(title: str) -> bool:
        title_lower = title.lower()
        return any(fuzz.partial_ratio(kw, title_lower) > threshold for kw in keywords_lower)
    
    filtered_df = video_df[video_df['title'].apply(matches_game)].copy()
    
    if filtered_df.empty:
        print(f"{Fore.YELLOW}No videos found for {selected_game}.{Style.RESET_ALL}")
        return
    
    # Display results
    print(f"\n{Fore.CYAN}=== Found {len(filtered_df)} videos for {selected_game} ==={Style.RESET_ALL}")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(filtered_df[['title', 'video_id', 'date']].to_string(index=False))
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')

def run_video_processing(youtube_client: YouTubeClient, custom_date: Optional[str] = None):
    """
    Main workflow to fetch, parse, filter, and upload video data.
    Args:
        youtube_client: An initialized YouTubeClient.
        custom_date: A specific start date (YYYY-MM-DD) to fetch from.
    """
    published_after_filter_date = custom_date or DEFAULT_PUBLISHED_AFTER_DATE
    print(f"Fetching videos published after: {published_after_filter_date}")

    raw_videos = fetch_and_process_videos(youtube_client, published_after_filter_date)
    
    if not raw_videos:
        print("No new videos found to process.")
        return

    video_df = parse_video_data(raw_videos)
    video_df.sort_values(by='date', ascending=False, inplace=True)
    
    filtered_df = fuzzy_filter_videos(video_df)
    
    if filtered_df.empty:
        print("No videos matched the filter criteria.")
        return

    print(f"\n{Fore.CYAN}--- Filtered Videos ---{Style.RESET_ALL}\n", filtered_df)
    update_video_sheet(filtered_df)

def interactive_menu(youtube_client: YouTubeClient):
    """Displays an interactive command-line menu for the user."""
    while True:
        print(f"\n{Fore.CYAN}--- RDC Video Bot Menu ---{Style.RESET_ALL}")
        print("1. Fetch and update videos (default)")
        print("2. Fetch stats from dashboard")
        print("3. Fetch videos from a specific date onwards")
        print("4. Show latest videos from sheet")
        print("5. Search videos by game from playlist")
        print(f"{Fore.RED}6. Exit{Style.RESET_ALL}")

        choice = input(f"{Fore.BLUE}Enter your choice (1-6): {Style.RESET_ALL}")

        if choice == '1':
            run_video_processing(youtube_client)
        elif choice == '2':
            display_dashboard_stats()
        elif choice == '3':
            date_input = input("Enter the date (YYYY-MM-DD): ")
            try:
                datetime.strptime(date_input, "%Y-%m-%d")
                run_video_processing(youtube_client, custom_date=date_input)
            except ValueError:
                print(f"{Fore.RED}Invalid date format. Please use YYYY-MM-DD.{Style.RESET_ALL}")
        elif choice == '4':
            display_latest_videos()
        elif choice == '5':
            search_game_videos_from_playlist(youtube_client)
        elif choice == '6':
            print(f"{Fore.RED}Exiting.{Style.RESET_ALL}")
            break
        else:
            print(f"{Fore.RED}Invalid choice. Please try again.{Style.RESET_ALL}")

def main():
    """Initializes resources and starts the interactive menu."""
    colorama_init(autoreset=True)
    load_dotenv()

    api_key = os.getenv("API_KEY")
    if not api_key:
        print(f"{Fore.RED}Error: API_KEY not found in .env file.{Style.RESET_ALL}")
        return

    try:
        youtube_client = YouTubeClient(api_key)
        interactive_menu(youtube_client)
    except ValueError as e:
        print(f"{Fore.RED}Initialization failed: {e}{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}An unexpected error occurred: {e}{Style.RESET_ALL}")

if __name__ == "__main__":
    main()