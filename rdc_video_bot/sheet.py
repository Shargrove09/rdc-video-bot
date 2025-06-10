import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from config import SPREADSHEET_NAME
from datetime import datetime # Added import



# TODO: Refactor to either remove this function or use it to set a new video sheet? 
#TODO: Should password lock rewriting 1st (main) sheet?
def set_video_sheet(fetched_video_frame): 
    gc = gspread.service_account()
    while True:
        sheet_num = input("Enter sheet number to rewrite (2 or higher): ")
        try:
            sheet_num = int(sheet_num)
            if sheet_num > 1:
                video_sheet = gc.open(SPREADSHEET_NAME).get_worksheet(sheet_num - 1)
                break
            else:
                print("Sheet number must be greater than 1")
        except ValueError:
            print("Please enter a valid number")

    print("Video data:")
    for _, row in fetched_video_frame.iterrows():
        print(row.to_string()) 
    set_with_dataframe(video_sheet, fetched_video_frame)

def update_dashboard_sheet(gc, videos_df_original): # gc is gspread client, videos_df is the dataframe from main sheet
    try:
        print("Updating dashboard sheet...")
        sh = gc.open(SPREADSHEET_NAME) 
        try:
            dashboard_sheet = sh.worksheet("Dashboard")
        except gspread.exceptions.WorksheetNotFound:
            print("Dashboard sheet not found, creating one.")
            # Create with a reasonable number of rows for stats and 2 columns
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

# Should update entire video sheet with new videos from RDC Live
# Shouldnt erase completed status of videos or any data 
def update_video_sheet(fetched_video_frame): 
    try: 
        gc = gspread.service_account()

        current_sheet = gc.open(SPREADSHEET_NAME).sheet1
        print("Current Sheet: ", current_sheet)
        # Convert boolean column to Sheets checkbox format

        current_df = get_as_dataframe(current_sheet)
        # Get current data and handle checkboxes
        current_df['added_to_db'] = current_df['added_to_db'].map({
            'TRUE': True,
            'FALSE': False,
            True: True,
            False: False
        })
            # Check if sheet is empty
        if current_df.empty or current_df.isna().all().all():
            print("Sheet is empty. Adding all videos as new.")
            updated_df = fetched_video_frame
            new_videos = fetched_video_frame

        else: 

            current_df = current_df.dropna(how='all')
            current_df['date'] = pd.to_datetime(current_df['date'])

            latest_date = current_df['date'].max()
            print("Latest Video Date: ", latest_date)

            if not current_df.empty:
                new_videos = fetched_video_frame[~fetched_video_frame['video_id'].isin(current_df['video_id'])]
                updated_df = pd.concat([current_df, new_videos], ignore_index=True)
            else: 
                updated_df = fetched_video_frame
        updated_df['added_to_db'] = updated_df['added_to_db'].map({
            True: 'TRUE',
            False: 'FALSE'
    })
        print(f"Adding {len(new_videos)} new videos")
        set_with_dataframe(current_sheet, updated_df)

        # Update dashboard sheet
        print("Attempting to update dashboard sheet...")
        update_dashboard_sheet(gc, updated_df) # Pass the gspread client and the updated DataFrame

        user_input = input("Would you like to see the dataframe (current_sheet) information? (y/n): ")
        if user_input.lower() == 'y':
            print_dataframe_info(current_df, "Current Sheet")

    except gspread.exceptions.SpreadsheetNotFound as e:
        print(f"Error: Spreadsheet not found. Please check the name and permissions")
    except Exception as e:
        print(f"Error: {str(e)}")

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


