"""
Interactive CLI entry point for the RDC Video Bot.
"""
import os
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from colorama import Fore, Style, init as colorama_init
from typing import Optional

from rdc_video_bot.sheet import update_video_sheet, fetch_dashboard_stats, fetch_latest_videos
from rdc_video_bot.config import DEFAULT_PUBLISHED_AFTER_DATE, get_games
from rdc_video_bot.core import YouTubeClient, fetch_and_process_videos, parse_video_data, fuzzy_filter_videos


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
            if stat.startswith('---'):
                color = Fore.CYAN
            elif 'not' in str(stat).lower() or 'error' in str(stat).lower():
                color = Fore.YELLOW
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

    print(f"\n{Fore.CYAN}=== Available Games ==={Style.RESET_ALL}")
    for idx, game in enumerate(games, 1):
        print(f"{idx}. {game}")

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

    date_input = input(
        f"{Fore.BLUE}Enter start date (YYYY-MM-DD) or press Enter for default ({DEFAULT_PUBLISHED_AFTER_DATE}): {Style.RESET_ALL}"
    ).strip()

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

    raw_videos = fetch_and_process_videos(youtube_client, published_after_date)

    if not raw_videos:
        print(f"{Fore.YELLOW}No videos found.{Style.RESET_ALL}")
        return

    video_df = parse_video_data(raw_videos)

    if video_df.empty:
        print(f"{Fore.YELLOW}No videos to process.{Style.RESET_ALL}")
        return

    matched_df = fuzzy_filter_videos(video_df)
    if matched_df.empty:
        filtered_df = pd.DataFrame()
    else:
        filtered_df = matched_df[matched_df['games'].str.contains(selected_game, na=False, regex=False)]

    if filtered_df.empty:
        print(f"{Fore.YELLOW}No videos found for {selected_game}.{Style.RESET_ALL}")
        return

    print(f"\n{Fore.CYAN}=== Found {len(filtered_df)} videos for {selected_game} ==={Style.RESET_ALL}")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(filtered_df[['title', 'video_id', 'date']].to_string(index=False))
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')


def run_video_processing(youtube_client: YouTubeClient, custom_date: Optional[str] = None):
    """Main workflow to fetch, parse, filter, and upload video data."""
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
    logging.basicConfig(level=logging.INFO, format='%(message)s')
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
