import json
from pathlib import Path

# --- Core API and Sheet Configuration ---
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
YOUTUBE_PLAYLIST_ID = "UUOnECY8FBKKPVi5ZsSgXPJA"
SPREADSHEET_NAME = "Project RDC Video Tracker"

# --- Fetching Behavior ---
MAX_PAGES_TO_FETCH = 25
DEFAULT_PUBLISHED_AFTER_DATE = "2025-02-02"

# --- Video Filter Configuration ---

def _load_video_filter_config() -> dict:
    """Loads the video filter configuration from a JSON file."""
    try:
        config_path = Path(__file__).parent / "video_filter.json"
        with open(config_path, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading video_filter.json: {e}")
        # Return an empty dict as a fallback to prevent crashes
        return {}

VIDEO_FILTER = _load_video_filter_config()

def get_games() -> list:
    """Returns a list of all configured game categories from the loaded filter."""
    return list(VIDEO_FILTER.keys())

# --- Column Schema Configuration ---
# Centralized definition of all columns used in the video tracking system
# Order matters - this determines the column order in the spreadsheet

VIDEO_COLUMNS = [
    "title",
    "video_id",
    "date",
    "added_to_db",
    "date_added_to_db",
    "has_screenshots",
    "date_screenshotted",
    "games",
    "Day Winner(s)"
]

# Number of columns to read from the sheet (automatically calculated)
NUM_COLUMNS = len(VIDEO_COLUMNS)

