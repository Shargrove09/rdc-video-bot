import logging
from datetime import datetime
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from rapidfuzz import fuzz
from typing import List, Dict, Set, Optional, Any

from rdc_video_bot.config import (
    VIDEO_FILTER, YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    YOUTUBE_PLAYLIST_ID, MAX_PAGES_TO_FETCH, DEFAULT_PUBLISHED_AFTER_DATE,
    VIDEO_COLUMNS
)

logger = logging.getLogger(__name__)


class YouTubeClient:
    """A client to interact with the YouTube Data API."""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("API key cannot be empty.")
        self.youtube = googleapiclient.discovery.build(
            YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=api_key
        )

    def fetch_playlist_page(self, page_token: Optional[str] = None) -> Optional[Dict[str, Any]]:
        try:
            request = self.youtube.playlistItems().list(
                part="snippet,contentDetails",
                maxResults=50,
                playlistId=YOUTUBE_PLAYLIST_ID,
                pageToken=page_token
            )
            return request.execute()
        except googleapiclient.errors.HttpError as e:
            logger.error(f"API error occurred: {e}")
            return None


def fetch_and_process_videos(youtube_client: YouTubeClient, published_after_str: str) -> List[Dict[str, Any]]:
    """Fetches and deduplicates all playlist videos published on or after the given date."""
    all_videos = []
    processed_video_ids: Set[str] = set()
    current_page_token: Optional[str] = None
    pages_fetched = 0

    try:
        target_date = datetime.strptime(published_after_str, "%Y-%m-%d").date()
    except ValueError:
        logger.warning(f"Invalid date format: '{published_after_str}'. Using default.")
        target_date = datetime.strptime(DEFAULT_PUBLISHED_AFTER_DATE, "%Y-%m-%d").date()

    while pages_fetched < MAX_PAGES_TO_FETCH:
        logger.info(f"Fetching page {pages_fetched + 1}...")
        response = youtube_client.fetch_playlist_page(current_page_token)

        if not response:
            break

        items = response.get('items', [])
        if not items:
            logger.info("No items found on this page.")
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
            logger.info(f"Reached videos older than {target_date}. Stopping.")
            break

        current_page_token = response.get('nextPageToken')
        if not current_page_token:
            logger.info("End of playlist reached.")
            break

        pages_fetched += 1
        if pages_fetched >= MAX_PAGES_TO_FETCH:
            logger.info(f"Reached max page fetch limit of {MAX_PAGES_TO_FETCH}.")

    return all_videos


def parse_video_data(videos: List[Dict[str, Any]]) -> pd.DataFrame:
    """Parses raw YouTube API video items into a structured DataFrame."""
    video_data_list = []
    for video in videos:
        title = video['snippet']['title']
        video_id = video['contentDetails']['videoId']
        date_str = video['contentDetails']['videoPublishedAt']
        date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")

        video_dict = {col: None for col in VIDEO_COLUMNS}
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
    Filters videos whose titles fuzzy-match any configured game keyword.
    Returns a DataFrame with a 'games' column listing matched games (comma-separated).
    """
    if videos_df.empty:
        return pd.DataFrame()

    game_keywords = {game: [kw.lower() for kw in keywords] for game, keywords in VIDEO_FILTER.items()}

    filtered_data = []
    for video_row in videos_df.itertuples(index=False):
        title_lower = video_row.title.lower()
        matched_games: Set[str] = set()

        for game, keywords in game_keywords.items():
            for keyword in keywords:
                if fuzz.partial_ratio(keyword, title_lower) > threshold:
                    matched_games.add(game)
                    break

        if matched_games:
            video_dict = video_row._asdict()
            video_dict['games'] = ', '.join(sorted(matched_games))
            filtered_data.append(video_dict)

    return pd.DataFrame(filtered_data)
