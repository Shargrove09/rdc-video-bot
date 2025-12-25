# RDC Video Bot

An automated YouTube video tracker that monitors Project RDC's channel, filters videos by game categories using fuzzy matching, and manages video data in Google Sheets.

## Overview

The RDC Video Bot fetches videos from a YouTube playlist, intelligently categorizes them by game using keyword matching, and tracks them in a Google Sheets spreadsheet. It offers both interactive and automated modes for flexible usage scenarios.

> **For Developers**: See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed technical documentation about components, data flow, and system design.

---

## Setup

### Prerequisites

1. **Python 3.8+** with pip and venv
2. **Google Cloud Project** with YouTube Data API v3 and Google Sheets API enabled
3. **Google Service Account** with credentials JSON file
4. **Google Spreadsheet** shared with the service account email

### 1. Environment Variables (.env file)

Create a `.env` file in the project root with the following variables:

```env
API_KEY=your_youtube_api_key_here
```

**Environment Variable Explanations**:

- **`API_KEY`**: Your YouTube Data API v3 key from Google Cloud Console
  - Used to authenticate requests to YouTube's API
  - Required for fetching playlist data
  - Generate at: Google Cloud Console → APIs & Services → Credentials → Create Credentials → API Key
  - Enable YouTube Data API v3 for your project before generating

### 2. Google Service Account Setup

A service account is a special type of Google account intended to represent a non-human user that needs to authenticate and be authorized to access data in Google APIs.

Since it's a separate account, by default it does not have access to any spreadsheet until you share it with this account. Just like any other Google account.

**Steps to create a service account**:

1. **Enable API Access** for your Google Cloud Project if you haven't done it yet.

2. Go to **"APIs & Services > Credentials"** and choose **"Create credentials > Service account key"**.

3. **Fill out the form** with a descriptive name for the service account.

4. Click **"Create"** and **"Done"**.

5. Press **"Manage service accounts"** above Service Accounts.

6. Press on **⋮** near the recently created service account and select **"Manage keys"**, then click **"ADD KEY > Create new key"**.

7. Select **JSON key type** and press **"Create"**.

8. You will automatically download a JSON file with credentials. It may look like this:

```json
{
  "type": "service_account",
  "project_id": "api-project-XXX",
  "private_key_id": "2cd … ba4",
  "private_key": "-----BEGIN PRIVATE KEY-----\nNrDyLw … jINQh/9\n-----END PRIVATE KEY-----\n",
  "client_email": "473000000000-yoursisdifferent@developer.gserviceaccount.com",
  "client_id": "473 … hd.apps.googleusercontent.com",
  ...
}
```

9. **Remember** the path to the downloaded credentials file. Also, in the next step you'll need the value of `client_email` from this file.

10. **⚠️ VERY IMPORTANT**: Go to your Google Spreadsheet and **share it** with the `client_email` from the step above. Just like you do with any other Google account. Give it **Editor** permissions.

    - If you don't do this, you'll get a `gspread.exceptions.SpreadsheetNotFound` exception when trying to access the spreadsheet.

11. **Move the downloaded file** to the appropriate location:
    - **Linux/Mac**: `~/.config/gspread/service_account.json`
    - **Windows**: `%APPDATA%\gspread\service_account.json`

### 3. Install Dependencies

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -e .
```

### 4. Configure Game Filters (Optional)

Edit [video_filter.json](rdc_video_bot/video_filter.json) to add or modify game categories and their keyword patterns:

```json
{
  "Game Name": ["keyword1", "keyword2", "phrase to match"]
}
```

The fuzzy matcher uses an 80% similarity threshold, so keywords don't need to be exact matches.

---

## Usage

### Interactive Mode

Run the interactive menu for manual operations:

```bash
python -m rdc_video_bot.main
```

Or use the batch file:

```bash
run_script.bat
```

**Menu Options**:

1. **Fetch and update videos** - Uses default date filter from config
2. **Display dashboard** - Shows statistics from Google Sheets
3. **Fetch from custom date** - Specify a custom start date
4. **Show latest videos** - Display recent videos from the sheet
5. **Search by game** - Filter videos by specific game category
6. **Exit**

### Automated Mode

For scheduled runs (Task Scheduler, cron jobs):

```bash
python -m rdc_video_bot.script [YYYY-MM-DD]
```

Examples:

```bash
# Use default date (yesterday)
python -m rdc_video_bot.script

# Specify custom date
python -m rdc_video_bot.script 2025-01-01
```

**With logging** (silent execution):

```bash
run_script_with_logging.bat
```

All logs are written to the `logs/` directory (rotating files, 10MB max, 5 backups)

---

## Project Structure

```
rdc-video-bot/
├── rdc_video_bot/
│   ├── __init__.py
│   ├── config.py              # Configuration management
│   ├── main.py                # Interactive CLI entry point
│   ├── script.py              # Automated/scheduled entry point
│   ├── sheet.py               # Google Sheets integration
│   └── video_filter.json      # Game categorization rules
├── tests/
│   └── __init__.py
├── logs/                       # Rotating log files (auto-created)
├── .env                        # Environment variables (create this)
├── client_secret_google.json  # Service account (move to config dir)
├── pyproject.toml             # Dependencies and project metadata
├── run_script.bat             # Interactive execution (Windows)
├── run_script_with_logging.bat # Silent execution (no console pause)
├── ARCHITECTURE.md            # Developer documentation
└── README.md
```

---

## Troubleshooting

### SpreadsheetNotFound Exception

- Ensure the spreadsheet name in [config.py](rdc_video_bot/config.py) matches exactly
- Verify you've shared the spreadsheet with the service account email (from `client_email` in JSON)
- Check that `service_account.json` is in the correct directory

### YouTube API Quota Exceeded

- Default quota is 10,000 units/day (each playlist request = 1 unit)
- Reduce `MAX_PAGES_TO_FETCH` in [config.py](rdc_video_bot/config.py)
- Request quota increase from Google Cloud Console

### No Videos Found After Filtering

- Check [video_filter.json](rdc_video_bot/video_filter.json) keywords match video titles
- Fuzzy matching uses 80% threshold - try more specific keywords
- Verify `DATE_FILTER` in [config.py](rdc_video_bot/config.py) isn't filtering out videos

### Import Errors

- Ensure virtual environment is activated
- Reinstall dependencies: `pip install -e .`
- Check Python version is 3.8+

---

## License

MIT License
