# RDC Video Bot - Architecture

Developer documentation for understanding the codebase structure, components, and data flow.

## High-Level Architecture

### Core Components

#### 1. **config.py** - Configuration Hub

Central configuration module that manages all system settings:

- YouTube API configuration (`API_KEY`, `YOUTUBE_API_SERVICE_NAME`, `YOUTUBE_API_VERSION`)
- Target playlist ID (`PLAYLIST_ID` - "UUOnECY8FBKKPVi5ZsSgXPJA" for Project RDC uploads)
- Google Sheets spreadsheet name (`SPREADSHEET_NAME` - "Project RDC Video Tracker")
- Fetching behavior parameters (`MAX_PAGES_TO_FETCH`, `DATE_FILTER`)
- Loads game categories from [video_filter.json](rdc_video_bot/video_filter.json)
- Provides `get_categories()` helper to retrieve available game categories

#### 2. **video_filter.json** - Game Detection Rules

JSON configuration defining game categories and their keyword patterns:

- Maps game names to arrays of keywords for fuzzy matching
- Currently configured games: MK8, MK World, COD (Black Ops 6), Rocket League, Lethal Company, Marvel Rivals
- Easily extensible - add new games by editing this file

#### 3. **main.py** - Interactive Entry Point

Primary user interface with 6-option CLI menu:

- **YouTubeClient class**: Wraps YouTube Data API v3
  - `fetch_playlist_page()`: Paginated API requests with date filtering
- **Core Functions**:
  - `fetch_all_videos()`: Orchestrates pagination, deduplication
  - `parse_video_data()`: Transforms API responses to DataFrames
  - `fuzzy_filter_videos()`: RapidFuzz matching (80% threshold) against game keywords
  - `fetch_and_update_videos()`: Complete ETL pipeline (Extract → Transform → Load)
  - `display_main_menu()`: Interactive menu system

**Menu Options**:

1. Fetch and update videos (default date)
2. Display dashboard statistics
3. Fetch from custom date
4. Show latest videos from sheet
5. Search videos by specific game
6. Exit

#### 4. **script.py** - Automated Entry Point

Non-interactive automation interface for scheduled runs:

- Accepts CLI date argument or defaults to yesterday
- Comprehensive logging infrastructure (rotating file handler in `logs/` directory)
- Executes `fetch_and_update_videos()` headlessly
- Designed for Task Scheduler / cron jobs
- Logging: 10MB max per file, 5 backup files

#### 5. **sheet.py** - Google Sheets Integration

Manages all spreadsheet operations via gspread service account:

- **SpreadsheetManager class**: Core sheet interface
  - `read_sheet_data()`: Reads columns A-G from main sheet
  - `update_sheet()`: Clears and writes DataFrame
  - `create_worksheet()`: Creates new worksheets

**Key Functions**:

- `update_video_sheet()`: Main update workflow
  - Fetches existing data
  - Normalizes DataFrame columns (booleans, dates)
  - Merges new videos with existing (deduplicates by `video_id`)
  - Sorts by date (descending)
  - Updates main sheet and Dashboard
- `update_dashboard()`: Writes statistics to Dashboard worksheet
- `get_video_stats()`: Calculates comprehensive metrics (total videos, unique IDs, date ranges, per-game counts)
- `get_dashboard_data()`: Retrieves dashboard for display
- `get_latest_videos()`: Returns top N recent videos

### Data Flow

```
┌─────────────────────────────────────────────────────────┐
│  ENTRY POINTS                                           │
├─────────────────────────────────────────────────────────┤
│  main.py (Interactive)      script.py (Automated)      │
│    ↓ Menu selection           ↓ CLI/Scheduled          │
└─────────────────────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────┐
│  EXTRACT: YouTube Data API                              │
├─────────────────────────────────────────────────────────┤
│  YouTubeClient.fetch_playlist_page()                    │
│    • Paginated requests (50 items/page, max 25 pages)  │
│    • Filter by publish date                             │
│    • Deduplicate by video_id                            │
└─────────────────────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────┐
│  TRANSFORM: Parse & Filter                              │
├─────────────────────────────────────────────────────────┤
│  parse_video_data() → fuzzy_filter_videos()            │
│    • Extract: title, video_id (URL), date              │
│    • Add: added_to_db, date_added_to_db fields         │
│    • Fuzzy match against video_filter.json keywords    │
│    • RapidFuzz partial_ratio (80% threshold)           │
│    • Add 'games' column (comma-separated matches)      │
└─────────────────────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────┐
│  LOAD: Google Sheets                                    │
├─────────────────────────────────────────────────────────┤
│  update_video_sheet()                                   │
│    • Fetch current sheet data                           │
│    • Merge: Identify new videos by video_id            │
│    • Sort by date (descending)                          │
│    • Write to main sheet (columns A-G)                  │
│    • Update Dashboard with statistics                   │
└─────────────────────────────────────────────────────────┘
```

### Component Interactions

```
┌──────────────┐
│   config.py  │──┐
└──────────────┘  │
                  ├─► Provides settings to all modules
┌──────────────┐  │
│video_filter. │──┘
│   json       │
└──────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   main.py    │────►│ YouTube API  │     │ Google Sheets│
│  (MenuUI)    │     │    Client    │     │     API      │
└──────────────┘     └──────────────┘     └──────────────┘
       │                     │                     │
       └─────────────────────┴─────────────────────┘
                             ▼
                  ┌──────────────────────┐
                  │ ETL Pipeline:        │
                  │ 1. fetch_all_videos  │
                  │ 2. parse_video_data  │
                  │ 3. fuzzy_filter      │
                  │ 4. update_sheet      │
                  └──────────────────────┘
                             │
                             ▼
┌──────────────┐     ┌──────────────┐
│  script.py   │────►│   sheet.py   │
│ (Automation) │     │ (Spreadsheet │
│              │     │   Manager)   │
└──────────────┘     └──────────────┘
```

### External Integrations

#### YouTube Data API v3

- **Purpose**: Fetch playlist video metadata
- **Authentication**: API key stored in `.env` file
- **Library**: `google-api-python-client`
- **Endpoint**: `playlistItems.list` with pagination
- **Rate Limiting**: Max 25 pages (~1,250 videos per run)

#### Google Sheets API

- **Purpose**: Store and track video data
- **Authentication**: Service account JSON (gspread)
- **Library**: `gspread` + `gspread-dataframe`
- **Spreadsheet**: "Project RDC Video Tracker"
- **Worksheets**:
  - **Main sheet** (sheet1): Video records with columns A-G
  - **Dashboard**: Statistics and metrics

### Data Schema

**Video DataFrame Columns**:

- `title`: Video title (string)
- `video_id`: Full YouTube URL (string)
- `date`: Publication date/time (formatted string)
- `added_to_db`: Boolean flag (TRUE/FALSE in sheet)
- `date_added_to_db`: Timestamp when marked as added (nullable)
- `games`: Comma-separated matched game categories (added by filter)

**Dashboard Statistics**:

- Last update timestamp
- Total videos, unique video count
- Videos added/not added to database
- Latest/oldest video information
- Date range timespan
- Per-game video counts

### Key Dependencies

- **google-auth-oauthlib** (1.x): OAuth2 authentication
- **google-api-python-client** (2.x): YouTube API client
- **pandas** (2.x): Data manipulation
- **python-dotenv** (1.x): Environment variable management
- **gspread** (5.x): Google Sheets API wrapper
- **gspread-dataframe** (3.x): DataFrame ↔ Sheets conversion
- **rapidfuzz** (3.x): Fuzzy string matching for game detection
- **colorama** (0.4.x): Cross-platform colored terminal output

See [pyproject.toml](pyproject.toml) for complete dependency list.
