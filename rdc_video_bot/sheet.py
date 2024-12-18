import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

def set_video_sheet(fetched_video_frame): 
    gc = gspread.service_account()

    video_sheet = gc.open("Project RDC Video Tracker").sheet1

    # current_df = get_as_dataframe(video_sheet);
    set_with_dataframe(video_sheet, fetched_video_frame)


if __name__ == "__main__": 
    pass 


   