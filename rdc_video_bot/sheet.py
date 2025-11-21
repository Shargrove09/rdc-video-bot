import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from datetime import datetime
import traceback
from typing import Optional, Tuple, Dict, Any, List

from config import SPREADSHEET_NAME

# --- Google Sheets Client Class ---

class GoogleSheetsClient:
    """A client to handle all interactions with Google Sheets."""

    def __init__(self, spreadsheet_name: str):
        """
        Initializes the client and connects to the spreadsheet.
        Args:
            spreadsheet_name: The name of the Google Sheet.
        """
        try:
            self.gc = gspread.service_account()
            self.spreadsheet = self.gc.open(spreadsheet_name)
            self.main_sheet = self.spreadsheet.sheet1
            print(f"--- Connected to Sheet: '{self.main_sheet.title}' in '{spreadsheet_name}' ---")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Error: Spreadsheet '{spreadsheet_name}' not found.")
            raise
        except Exception as e:
            print(f"An unexpected error occurred during Google Sheets client initialization: {e}")
            raise

    def get_sheet_as_dataframe(self, sheet_name: str) -> Optional[pd.DataFrame]:
        """Fetches a worksheet as a pandas DataFrame."""
        try:
            sheet = self.spreadsheet.worksheet(sheet_name)
            df = get_as_dataframe(sheet, evaluate_formulas=True)
            if df is not None and not df.empty:
                df = df.dropna(how='all').reset_index(drop=True)
            return df
        except gspread.exceptions.WorksheetNotFound:
            return None
        except Exception as e:
            print(f"Error fetching sheet '{sheet_name}': {e}")
            return None

    def write_dataframe_to_sheet(self, sheet_name: str, df: pd.DataFrame):
        """Clears a sheet and writes a DataFrame to it."""
        try:
            sheet = self.spreadsheet.worksheet(sheet_name)
            sheet.clear()
            set_with_dataframe(sheet, df, include_index=False, resize=True)
            print(f"Sheet '{sheet_name}' updated successfully.")
        except gspread.exceptions.WorksheetNotFound:
            print(f"Cannot write to sheet '{sheet_name}': Not found.")
        except Exception as e:
            print(f"Error writing to sheet '{sheet_name}': {e}")

    def create_sheet(self, sheet_name: str) -> Optional[gspread.Worksheet]:
        """Creates a new worksheet if it doesn't exist."""
        try:
            sheet = self.spreadsheet.add_worksheet(title=sheet_name, rows=20, cols=2)
            print(f"Created new sheet: '{sheet_name}'")
            return sheet
        except gspread.exceptions.APIError as e:
            if 'already exists' in str(e):
                return self.spreadsheet.worksheet(sheet_name)
            print(f"API Error creating sheet '{sheet_name}': {e}")
            return None

# --- Dashboard Statistics Logic ---

def _calculate_dashboard_stats(videos_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates statistics from the video DataFrame for the dashboard."""
    if videos_df.empty:
        return pd.DataFrame([("No data available", "")], columns=["Statistic", "Value"])

    stats = {
        "Total Videos": int(len(videos_df)),
        "Videos Marked 'added_to_db'": int(videos_df['added_to_db'].astype(str).str.upper().eq('TRUE').sum()),
        "Unique Video IDs": int(videos_df['video_id'].nunique()),
    }
    stats["Videos Not Marked 'added_to_db'"] = int(stats["Total Videos"] - stats["Videos Marked 'added_to_db'"])

    # Date-based stats
    if 'date' in videos_df.columns:
        valid_dates_df = videos_df.dropna(subset=['date']).sort_values(by='date', ascending=False)
        if not valid_dates_df.empty:
            latest = valid_dates_df.iloc[0]
            oldest = valid_dates_df.iloc[-1]
            stats["Latest Video Title"] = latest.get('title', "N/A")
            stats["Latest Video Date"] = latest['date'].strftime("%Y-%m-%d %H:%M:%S")
            stats["Oldest Video Title"] = oldest.get('title', "N/A")
            stats["Oldest Video Date"] = oldest['date'].strftime("%Y-%m-%d %H:%M:%S")
            if len(valid_dates_df) > 1:
                stats["Timespan of Videos (Days)"] = int((valid_dates_df['date'].max() - valid_dates_df['date'].min()).days)

    # Game stats
    game_counts = pd.Series([game.strip() for games_str in videos_df['games'].dropna() for game in games_str.split(',')]).value_counts()

    # Format for display
    dashboard_list = [("--- General Information ---", ""),
                      ("Last Dashboard Update", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                      ("", ""), ("--- Video Statistics ---", "")]
    dashboard_list.extend(stats.items())
    
    if not game_counts.empty:
        dashboard_list.append(("", ""))
        dashboard_list.append(("--- Game Statistics ---", ""))
        dashboard_list.extend([(f"Videos for {game}", int(count)) for game, count in game_counts.items()])

    return pd.DataFrame(dashboard_list, columns=["Statistic", "Value"])

def update_dashboard_sheet(client: GoogleSheetsClient, videos_df: pd.DataFrame):
    """Updates the 'Dashboard' sheet with statistics."""
    print("Updating dashboard sheet...")
    dashboard_df = _calculate_dashboard_stats(videos_df)
    
    dashboard_sheet = client.spreadsheet.worksheet("Dashboard")
    if not dashboard_sheet:
        dashboard_sheet = client.create_sheet("Dashboard")

    if dashboard_sheet:
        client.write_dataframe_to_sheet("Dashboard", dashboard_df)

# --- Main Video Sheet Update Logic ---

def _normalize_dataframe_columns(df: pd.DataFrame, df_name: str = "DataFrame") -> pd.DataFrame:
    """Normalizes 'added_to_db' and 'date' columns."""
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    if 'added_to_db' not in df.columns:
        df['added_to_db'] = False
    df['added_to_db'] = df['added_to_db'].astype(str).str.upper().map({'TRUE': True, 'FALSE': False}).fillna(False)

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    return df

def _merge_video_dataframes(current_df: pd.DataFrame, fetched_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Merges current and fetched data, identifying new videos."""
    if fetched_df.empty:
        return current_df.copy(), pd.DataFrame()

    if current_df.empty:
        return fetched_df.copy(), fetched_df.copy()

    # Ensure video_id types are consistent
    current_df['video_id'] = current_df['video_id'].astype(str)
    fetched_df['video_id'] = fetched_df['video_id'].astype(str)

    new_videos_df = fetched_df[~fetched_df['video_id'].isin(current_df['video_id'])].copy()
    
    if not new_videos_df.empty:
        updated_df = pd.concat([current_df, new_videos_df], ignore_index=True)
    else:
        updated_df = current_df.copy()
        
    return updated_df, new_videos_df

def _finalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Sorts and formats the DataFrame for writing to the sheet."""
    if df.empty:
        return df

    df['added_to_db'] = df['added_to_db'].map({True: 'TRUE', False: 'FALSE'}).fillna('FALSE')

    if 'date' in df.columns:
        df = df.sort_values(by='date', ascending=False, na_position='last').reset_index(drop=True)
        
    return df

def update_video_sheet(fetched_video_frame: pd.DataFrame):
    """
    Updates the main video sheet with new videos.
    Args:
        fetched_video_frame: DataFrame with newly fetched videos.
    """
    try:
        client = GoogleSheetsClient(SPREADSHEET_NAME)

        current_df_raw = client.get_sheet_as_dataframe(client.main_sheet.title)
        current_df = _normalize_dataframe_columns(current_df_raw, "Current Sheet Data")
        
        fetched_df = _normalize_dataframe_columns(fetched_video_frame.copy(), "Fetched Video Data")
        
        updated_df, new_videos_df = _merge_video_dataframes(current_df, fetched_df)
        
        final_df = _finalize_dataframe(updated_df)
        
        if not final_df.equals(current_df):
            client.write_dataframe_to_sheet(client.main_sheet.title, final_df)
            print(f"Found and added {len(new_videos_df)} new videos.")
        else:
            print("No new unique videos found to add.")

        update_dashboard_sheet(client, final_df.copy())

    except (gspread.exceptions.SpreadsheetNotFound, gspread.exceptions.APIError) as e:
        print(f"A Google Sheets error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in update_video_sheet: {e}")
        print(traceback.format_exc())

def fetch_dashboard_stats() -> Optional[pd.DataFrame]:
    """Fetches and returns the dashboard statistics as a DataFrame."""
    try:
        client = GoogleSheetsClient(SPREADSHEET_NAME)
        dashboard_df = client.get_sheet_as_dataframe("Dashboard")
        
        if dashboard_df is None:
            print("Dashboard sheet not found. Run option 1 to create it.")
            return None
        if dashboard_df.empty:
            print("Dashboard is empty.")
            return pd.DataFrame()
            
        return dashboard_df
    except Exception as e:
        print(f"Failed to fetch dashboard stats: {e}")
        return None

def fetch_latest_videos(limit: int = 10) -> Optional[pd.DataFrame]:
    """
    Fetches the latest 'limit' rows from the main video sheet.
    Args:
        limit: The number of rows to fetch.
    Returns:
        A DataFrame containing the latest videos.
    """
    try:
        client = GoogleSheetsClient(SPREADSHEET_NAME)
        df = client.get_sheet_as_dataframe(client.main_sheet.title)
        
        if df is None or df.empty:
            print("Main sheet is empty.")
            return None
            
        # Filter out unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        # Assuming the sheet is sorted by date descending, return the top 'limit' rows
        return df.head(limit)
    except Exception as e:
        print(f"Failed to fetch latest videos: {e}")
        return None

if __name__ == "__main__":
    # Example usage for testing purposes
    # You would need to create a sample DataFrame to test update_video_sheet
    pass


