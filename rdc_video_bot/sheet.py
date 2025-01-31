import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

def set_video_sheet(fetched_video_frame): 
    gc = gspread.service_account()

    video_sheet = gc.open("Project RDC Video Tracker").sheet1

    # current_df = get_as_dataframe(video_sheet);
    print("Video data:")
    for _, row in fetched_video_frame.iterrows():
        print(row.to_string())
    set_with_dataframe(video_sheet, fetched_video_frame)

# Should update entire video sheet with new videos from RDC Live
# Shouldnt erase completed status of videos or any data 
def update_video_sheet(): 
    try: 
        gc = gspread.service_account()

        current_sheet = gc.open("Project RDC Video Tracker").sheet1
        print("Current Sheet: ", current_sheet)
        current_df = get_as_dataframe(current_sheet)

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


   