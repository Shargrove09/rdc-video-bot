# YouTube API configuration
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
YOUTUBE_PLAYLIST_ID = "UUOnECY8FBKKPVi5ZsSgXPJA"

# Fetching configuration
MAX_PAGES_TO_FETCH = 25
DEFAULT_PUBLISHED_AFTER_DATE = "2025-02-02"

SPREADSHEET_NAME = "Project RDC Video Tracker"

"""Video filter configurations mapping game categories to search keywords."""

VIDEO_FILTER = {
    "MK8": ["MK8", "Mario Kart 8", "Mario Kart 8 Deluxe"],
    "COD": [
        "COD", 
        "Call of Duty", 
        "Call of Duty Warzone", 
        "Call of Duty Black Ops Cold War",
        "Black Ops 6",
    ],
    "Rocket League": ["Rocket League"],
}

def get_games():
    """Return a list of all configured game categories."""
    return list(VIDEO_FILTER.keys())


