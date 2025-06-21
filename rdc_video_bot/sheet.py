import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from config import SPREADSHEET_NAME
from datetime import datetime # Added import
import traceback # Added for more detailed error logging

def update_dashboard_sheet(gc, videos_df_original): # gc is gspread client, videos_df is the dataframe from main sheet
    try:
        print("Updating dashboard sheet...")
        sh = gc.open(SPREADSHEET_NAME) 
        try:
            dashboard_sheet = sh.worksheet("Dashboard")
        except gspread.exceptions.WorksheetNotFound:
            print("Dashboard sheet not found, creating one.")
            # Create basic dashboard sheet if it doesn't exist
            dashboard_sheet = sh.add_worksheet(title="Dashboard", rows="20", cols="2")

        videos_df = videos_df_original.copy() # Work with a copy to avoid modifying the original DataFrame

        # Initialize statistics
        total_videos = 0
        videos_in_db_count = 0
        videos_not_in_db_count = 0
        latest_video_title = "N/A"
        latest_video_date_str = "N/A"
        oldest_video_title = "N/A"
        oldest_video_date_str = "N/A"
        timespan_days = None
        unique_ids_count = None
        game_counts = pd.Series(dtype=int) # For storing counts of each game

        if not videos_df.empty:
            # Remove any fully empty rows, then count the remaining rows
            videos_df = videos_df.dropna(how='all')
            total_videos = len(videos_df)
            
            if 'added_to_db' in videos_df.columns:
                # Ensure the column is treated as string for comparison, as it comes from main sheet processing
                videos_df['added_to_db'] = videos_df['added_to_db'].astype(str)
                videos_in_db_count = videos_df[videos_df['added_to_db'].str.upper() == 'TRUE'].shape[0]
            else:
                print("Warning (Dashboard): 'added_to_db' column missing in DataFrame.")
            videos_not_in_db_count = total_videos - videos_in_db_count

            if 'date' in videos_df.columns and not videos_df['date'].isna().all():
                # Convert 'date' column to datetime objects, coercing errors to NaT
                videos_df['date'] = pd.to_datetime(videos_df['date'], errors='coerce')
                # Filter out rows where date conversion failed (NaT)
                valid_dates_df = videos_df.dropna(subset=['date'])
                
                if not valid_dates_df.empty:
                    # Sort by date to easily get latest and oldest
                    sorted_by_date_df = valid_dates_df.sort_values(by='date', ascending=False)
                    
                    latest_video_row = sorted_by_date_df.iloc[0]
                    latest_video_title = latest_video_row.get('title', "N/A")
                    latest_video_date_str = latest_video_row['date'].strftime("%Y-%m-%d %H:%M:%S")

                    oldest_video_row = sorted_by_date_df.iloc[-1]
                    oldest_video_title = oldest_video_row.get('title', "N/A")
                    oldest_video_date_str = oldest_video_row['date'].strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Calculate timespan if there's more than one unique date
                    if len(valid_dates_df['date'].unique()) > 1:
                        timespan_days = (valid_dates_df['date'].max() - valid_dates_df['date'].min()).days
                    elif len(valid_dates_df['date'].unique()) == 1:
                        timespan_days = 0 # Only one unique date, so timespan is 0
                else:
                    print("Warning (Dashboard): No valid dates found in 'date' column after conversion.")
            else:
                print("Warning (Dashboard): 'date' column missing or contains all invalid date values.")

            if 'video_id' in videos_df.columns:
                unique_ids_count = videos_df['video_id'].nunique()
            else:
                print("Warning (Dashboard): 'video_id' column missing in DataFrame.")

            # Calculate game statistics
            if 'games' in videos_df.columns and not videos_df['games'].isna().all():
                all_games_list = []
                # Split comma-separated games and count them
                for games_str in videos_df['games'].dropna():
                    all_games_list.extend([game.strip() for game in games_str.split(',')])
                if all_games_list:
                    game_counts = pd.Series(all_games_list).value_counts()
            else:
                print("Warning (Dashboard): 'games' column missing or empty in DataFrame.")


        # Prepare data for the dashboard sheet
        dashboard_data_list = [
            ("--- General Information ---", ""),
            ("Last Dashboard Update", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("", ""), # Spacer
            ("--- Video Statistics ---", ""),
            ("Total Videos in Sheet", total_videos),
            ("Videos Marked 'added_to_db'", videos_in_db_count),
            ("Videos Not Marked 'added_to_db'", videos_not_in_db_count),
        ]

        if unique_ids_count is not None:
            dashboard_data_list.append(("Unique Video IDs", unique_ids_count))

        dashboard_data_list.extend([
            ("", ""), # Spacer
            ("--- Video Details (by Publication Date) ---", ""),
            ("Latest Video Title", latest_video_title),
            ("Latest Video Date", latest_video_date_str),
            ("Oldest Video Title", oldest_video_title),
            ("Oldest Video Date", oldest_video_date_str),
        ])
        
        if timespan_days is not None:
            dashboard_data_list.append(("Timespan of Videos (Days)", timespan_days))
        
        # Add game statistics to dashboard data
        if not game_counts.empty:
            dashboard_data_list.append(("", "")) # Spacer
            dashboard_data_list.append(("--- Game Statistics ---", ""))
            for game, count in game_counts.items():
                dashboard_data_list.append((f"Videos for {game}", count))
        
        dashboard_df_to_write = pd.DataFrame(dashboard_data_list, columns=["Statistic", "Value"])
        
        # Clear the sheet and write the new dashboard data
        dashboard_sheet.clear() 
        set_with_dataframe(dashboard_sheet, dashboard_df_to_write, include_index=False, include_column_header=True, resize=True)
        print("Dashboard sheet updated successfully.")

    except gspread.exceptions.APIError as e:
        print(f"Error updating dashboard sheet (APIError): {str(e)}")
        # Specific advice for rate limiting
        if hasattr(e, 'response') and e.response.status_code == 429:
            print("This might be due to Google Sheets API rate limits. Consider adding delays if updates are frequent.")
    except Exception as e:
        print(f"An unexpected error occurred while updating dashboard sheet: {str(e)}")

# Helper Functions for update_video_sheet

def _setup_google_sheets_connection():
    """Initializes gspread client and opens the main video sheet."""
    gc = gspread.service_account()
    spreadsheet = gc.open(SPREADSHEET_NAME)
    current_sheet = spreadsheet.sheet1
    print(f"--- Connecting to Sheet: '{current_sheet.title}' in Spreadsheet: '{SPREADSHEET_NAME}' ---")
    return gc, current_sheet


def _normalize_dataframe_columns(df, df_name="DataFrame"):
    """
    Normalizes 'added_to_db' and 'date' columns in a DataFrame.
    This helper function ensures consistent formatting for key columns:
    - For 'added_to_db': Ensures it exists and contains boolean values
    - For 'date': Converts to datetime objects with proper error handling
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to normalize. Can be None or empty.
    df_name : str, default="DataFrame"
        Name identifier for the DataFrame, used in warning messages.
    Returns:
    --------
    pandas.DataFrame
        The normalized DataFrame with consistent column formats.
        If input df is None, returns empty DataFrame.
        If input df is empty, returns the empty DataFrame.
    Notes:
    ------
    - When 'added_to_db' column is missing, it's created with False values
    - Values in 'added_to_db' are normalized to boolean: 'TRUE'/True → True, 'FALSE'/False → False
    - Date parsing uses pd.to_datetime with errors='coerce' (invalid dates become NaT)
    - Warnings are printed when dates fail to parse or 'date' column is missing
    """
    """Normalizes 'added_to_db' and 'date' columns in a DataFrame."""
    if df is None or df.empty:
        # print(f"Info ({df_name}): DataFrame is empty or None, skipping normalization.")
        return pd.DataFrame() if df is None else df

    # Normalize 'added_to_db'
    if 'added_to_db' not in df.columns:
        df['added_to_db'] = False
        # print(f"Info ({df_name}): 'added_to_db' column added and set to False.")
    else:
        df['added_to_db'] = df['added_to_db'].astype(str).str.upper().map({
            'TRUE': True, 'FALSE': False, True: True, False: False
        }).fillna(False).infer_objects(copy=False)

    # Normalize 'date'
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if df['date'].isna().any():
            print(f"Warning ({df_name}): Some dates could not be parsed and were set to NaT.")
    else:
        print(f"Warning ({df_name}): 'date' column missing.")
        
    return df

def _get_current_sheet_data(current_sheet):
    """Fetches and prepares data from the current Google Sheet."""
    current_df = get_as_dataframe(current_sheet, evaluate_formulas=True)
    if current_df is None:
        print("Info (Current Sheet): Sheet is truly empty, initializing as empty DataFrame.")
        current_df = pd.DataFrame()
    elif not current_df.empty:
        current_df = current_df.dropna(how='all').reset_index(drop=True)
        
    return _normalize_dataframe_columns(current_df, "Current Sheet Data")

def _prepare_fetched_data(fetched_video_frame):
    """Prepares the newly fetched video DataFrame."""
    if fetched_video_frame is None:
        print("Warning (Fetched Data): fetched_video_frame is None. Returning empty DataFrame.")
        return pd.DataFrame()
    fetched_df = fetched_video_frame.copy()
    return _normalize_dataframe_columns(fetched_df, "Fetched Video Data")

def _merge_video_dataframes(current_df, fetched_df):
    """Merges current and fetched video data, identifying new videos."""
    new_videos_df = pd.DataFrame()

    if fetched_df.empty:
        print("Info (Merge): Fetched data is empty. No new videos to process.")
        return current_df.copy(), new_videos_df

    if current_df.empty:
        print("Info (Merge): Current sheet is empty. Adding all fetched videos as new.")
        updated_df = fetched_df.copy()
        new_videos_df = fetched_df.copy()
    else:
        if 'video_id' not in current_df.columns:
            print("Warning (Merge): 'video_id' column missing in the current sheet. Appending all fetched videos.")
            new_videos_df = fetched_df.copy()
            updated_df = pd.concat([current_df, new_videos_df], ignore_index=True)
        elif 'video_id' not in fetched_df.columns:
            print("Warning (Merge): 'video_id' column missing in fetched videos. No new videos can be added.")
            updated_df = current_df.copy() # No changes
        else:
            # Ensure video_id types are consistent for comparison
            current_df['video_id'] = current_df['video_id'].astype(str)
            fetched_df['video_id'] = fetched_df['video_id'].astype(str)

            new_videos_df = fetched_df[
                ~fetched_df['video_id'].isin(current_df['video_id'])
            ].copy()

            if not new_videos_df.empty:
                print(f"Info (Merge): Found {len(new_videos_df)} new videos to add.")
                updated_df = pd.concat([current_df, new_videos_df], ignore_index=True)
            else:
                print("Info (Merge): No new unique videos found.")
                updated_df = current_df.copy()
                
    return updated_df, new_videos_df

def _finalize_updated_dataframe(updated_df):
    """Finalizes the updated DataFrame (sorting, 'added_to_db' string conversion)."""
    if updated_df.empty:
        return updated_df

    # Final processing for 'added_to_db' before writing to sheet
    if 'added_to_db' not in updated_df.columns:
         updated_df['added_to_db'] = False # Safeguard
    
    updated_df['added_to_db'] = updated_df['added_to_db'].map({
        True: 'TRUE', False: 'FALSE'
    }).fillna('FALSE')

    # Sort DataFrame by date (descending), handling potential NaT values
    if 'date' in updated_df.columns:
        # Ensure it's datetime before sorting, though _normalize_dataframe_columns should handle this
        updated_df['date'] = pd.to_datetime(updated_df['date'], errors='coerce') 
        updated_df = updated_df.sort_values(by='date', ascending=False, na_position='last').reset_index(drop=True)
        # Optional: Convert date to string for sheet appearance
        # updated_df['date'] = updated_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('N/A')
    else:
        print("Warning (Finalize): 'date' column not found for sorting.")
        
    return updated_df

def _write_df_to_sheet_and_update_dashboard(current_sheet, updated_df, new_videos_count, gc_client):
    """Writes the DataFrame to the sheet and updates the dashboard."""
    if updated_df.empty and new_videos_count == 0: # Check new_videos_count as well
        print("Info (Write): Updated DataFrame is empty and no new videos. Sheet will not be cleared or updated.")
    else:
        print(f"Updating sheet with {len(updated_df)} total videos ({new_videos_count} new).")
        current_sheet.clear()
        set_with_dataframe(current_sheet, updated_df, include_index=False, resize=True)
        print("Main sheet updated successf ully.")

    print("Attempting to update dashboard sheet...")
    update_dashboard_sheet(gc_client, updated_df.copy()) # Pass a copy

def _offer_dataframe_info(df, df_name="Updated Sheet Data"):
    """Optionally prints detailed DataFrame information based on user input."""
    user_input = input(f"Would you like to see the {df_name} information? (y/n): ")
    if user_input.lower() == 'y':
        print_dataframe_info(df, df_name)

def _handle_update_video_sheet_errors(e):
    """Handles errors for the update_video_sheet function."""
    if isinstance(e, gspread.exceptions.SpreadsheetNotFound):
        print(f"Error: Spreadsheet '{SPREADSHEET_NAME}' not found. Please check the name and permissions.")
    elif isinstance(e, gspread.exceptions.APIError):
        print(f"Google Sheets API Error: {str(e)}")
        if hasattr(e, 'response') and e.response is not None and hasattr(e.response, 'status_code') and e.response.status_code == 429:
            print("This might be due to Google Sheets API rate limits. Consider adding delays or batching updates if frequent.")
    else:
        print(f"An unexpected error occurred in update_video_sheet: {str(e)}")
        print(traceback.format_exc())

def fetch_dashboard_stats():
    """
    Fetches the dashboard sheet data and returns it as a formatted DataFrame.
    
    Returns:
        pd.DataFrame: A DataFrame containing the dashboard statistics.
        None: If an error occurs or the dashboard sheet does not exist.
    """
    import gspread
    import pandas as pd
    import traceback
    
    try:
        print("Fetching dashboard statistics...")
        gc = gspread.service_account()
        sh = gc.open(SPREADSHEET_NAME)
        
        try:
            dashboard_sheet = sh.worksheet("Dashboard")
        except gspread.exceptions.WorksheetNotFound:
            print("Dashboard sheet not found. Please run option 1 first to create and populate the dashboard.")
            return None
            
        # Get data from dashboard sheet
        dashboard_data = get_as_dataframe(dashboard_sheet)
        
        # Clean the DataFrame
        if dashboard_data is not None and not dashboard_data.empty:
            # Remove completely empty rows and columns
            dashboard_data = dashboard_data.dropna(how='all').dropna(axis=1, how='all')
            
            # Reset the index for better display
            dashboard_data = dashboard_data.reset_index(drop=True)
            
            print("Dashboard statistics fetched successfully.")
            return dashboard_data
        else:
            print("Dashboard sheet exists but contains no data.")
            return None
            
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Spreadsheet '{SPREADSHEET_NAME}' not found. Please check the name and permissions.")
        return None
    except gspread.exceptions.APIError as e:
        print(f"Google Sheets API Error: {str(e)}")
        if hasattr(e, 'response') and e.response.status_code == 429:
            print("This might be due to Google Sheets API rate limits. Consider adding delays between requests.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while fetching dashboard stats: {str(e)}")
        print(traceback.format_exc())
        return None

def update_video_sheet(fetched_video_frame, show_detailed_info=False):
    """
    Updates the main video sheet with new videos from fetched_video_frame.
    It preserves existing video data and their 'added_to_db' status,
    and appends new, unique videos. The sheet is sorted by date descending.

    Args:
        fetched_video_frame (pd.DataFrame): DataFrame containing newly fetched videos.
                                            Expected to have 'video_id' and other relevant columns.
        show_detailed_info (bool): If True, prompts to display detailed DataFrame info.
    """
    try:
        gc, current_sheet = _setup_google_sheets_connection()

        current_df = _get_current_sheet_data(current_sheet)
        fetched_df = _prepare_fetched_data(fetched_video_frame)
        
        updated_df, new_videos_df = _merge_video_dataframes(current_df, fetched_df)
        
        final_updated_df = _finalize_updated_dataframe(updated_df)
        
        _write_df_to_sheet_and_update_dashboard(current_sheet, final_updated_df, len(new_videos_df), gc)

        if show_detailed_info:
            _offer_dataframe_info(final_updated_df)

    except Exception as e:
        _handle_update_video_sheet_errors(e)
            
"""Prints detailed information about a DataFrame including shape, columns, data types, first few rows, missing values, unique values per column, and basic statistics."""
def print_dataframe_info(df, name="DataFrame"):
    print(f"\n=== {name} Information ===")
    print("\nShape:", df.shape)
    print("\nColumns:", df.columns.tolist())
    print("\nData Types:\n", df.dtypes)
    print("\nFirst 5 rows:\n", df.head())
    print("\nMissing Values:\n", df.isnull().sum())
    print("\nUnique Values per Column:")
    for column in df.columns:
        print(f"{column}: {df[column].nunique()} unique values")
    print("\nBasic Statistics:\n", df.describe(include='all'))
    print("="*50)

if __name__ == "__main__": 
    pass


