import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd

global spreadsheet_name
spreadsheet_name = "Work Tracker"

# TODO: Refactor to either remove this function or use it to set a new video sheet? 
#TODO: Should password lock rewriting 1st (main) sheet?
def set_video_sheet(fetched_video_frame): 
    gc = gspread.service_account()
    while True:
        sheet_num = input("Enter sheet number to rewrite (2 or higher): ")
        try:
            sheet_num = int(sheet_num)
            if sheet_num > 1:
                video_sheet = gc.open(spreadsheet_name).get_worksheet(sheet_num - 1)
                break
            else:
                print("Sheet number must be greater than 1")
        except ValueError:
            print("Please enter a valid number")

    print("Video data:")
    for _, row in fetched_video_frame.iterrows():
        print(row.to_string()) 
    set_with_dataframe(video_sheet, fetched_video_frame)

# Should update entire video sheet with new videos from RDC Live
# Shouldnt erase completed status of videos or any data 
def update_video_sheet(fetched_video_frame): 
    try: 
        gc = gspread.service_account()

        current_sheet = gc.open(spreadsheet_name).sheet1
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

        user_input = input("Would you like to see the dataframe (current_sheet) information? (y/n): ")
        if user_input.lower() == 'y':
            print_dataframe_info(current_df, "Current Sheet")

    except gspread.exceptions.SpreadsheetNotFound as e:
        print(f"Error: Spreadsheet not found. Please check the name and permissions")
    except Exception as e:
        print(f"Error: {str(e)}")

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


   