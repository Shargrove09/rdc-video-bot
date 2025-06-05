import os
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from dotenv import load_dotenv
from sheet import set_video_sheet, update_video_sheet
from datetime import datetime
from rapidfuzz import fuzz, process

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

load_dotenv()

def fetchVideosFromPlaylist(youtube, pageToken=None, proccessed_videos = None): 
    if proccessed_videos is None: 
        proccessed_videos = set()
    # Fetch Playlist Results
    if (youtube): 
        playlist_request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            maxResults=50, # 50 is max limit set by YT API 
            playlistId="UUOnECY8FBKKPVi5ZsSgXPJA",
            pageToken=pageToken
        )
        playlist_results = playlist_request.execute(); 
        print(playlist_results) 

        print(f"Fetched {len(playlist_results.get('items', []))} videos")
        print(f"Page Token: {pageToken}")

        filtered_videos = []
        for item in playlist_results.get('items', []):
            video_id = item['contentDetails']['videoId']
            if video_id not in proccessed_videos:
                proccessed_videos.add(video_id)
                filtered_videos.append(item)

        playlist_results['items'] = filtered_videos
        try: 
            global page_token
            page_token = playlist_results.get('nextPageToken', "")
        except: 
            print("\n Couldn't find page token!")
            page_token = ""
        return playlist_results
    else:  
        print("Youtube Client not initialized!")

def parse_videos(playlist_results, video_data_list): 
    for video in playlist_results.get('items', []): 
        title = video['snippet']['title']
        video_id = video['contentDetails']['videoId']
        print(f"Title: {title}\nVideo ID: {video_id}\n")
        content_details = video['contentDetails']

        date_str = content_details['videoPublishedAt']
        date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        formatted_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")

        video_data_list.append({
            "title": title,
            "video_id": "https://www.youtube.com/watch?v=" + video_id,
            "date": formatted_date,
            "added_to_db": False,
            "date_added_to_db": None
        }) 

def main():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"

    api_key= os.getenv("API_KEY")
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key)
    video_data = []
    global page_token
    page_token = None
    
    for i in range(25): 
        fetched_videos = fetchVideosFromPlaylist(youtube, page_token)
        parse_videos(fetched_videos, video_data)
        i += 1


    video_data.sort(key=lambda x: x['date'], reverse=True)
    df = pd.DataFrame(video_data)
    filtered_df = filter_videos(df)
    print("--- \n Filtered DF \n --- \n", filtered_df)
    set_video_sheet(filtered_df)

video_filter = { 
    "MK8": ["MK8", "Mario Kart 8", "Mario Kart 8 Deluxe"],
    "COD": ["COD", "Call of Duty", "Call of Duty Warzone", "Call of Duty Black Ops Cold War", "Call of Duty Black Ops 6", "Black Ops 6"],
    "Rocket League": ["Rocket League",],
}

def testBedMain(): 
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"

    api_key= os.getenv("API_KEY")
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key)
    video_data = []
    
    global page_token
    page_token = None
    processed_videos = set()

    for i in range(25): 
        fetched_videos = fetchVideosFromPlaylist(youtube, page_token, processed_videos)
        parse_videos(fetched_videos, video_data)
        i += 1
    
    video_data.sort(key=lambda x: x['date'], reverse=True)
    df = pd.DataFrame(video_data)
    filtered_df = fuzzy_filter_videos(df)
    print("--- \n Filtered DF \n --- \n", filtered_df)
    update_video_sheet(filtered_df)

def filter_videos(videos): 
    filtered_videos = []
    for _, video in videos.iterrows():
        for game, keywords in video_filter.items():
            if any(keyword.lower() in video['title'].lower() for keyword in keywords):
                video['game'] = game
                filtered_videos.append(video)
                break
    return pd.DataFrame(filtered_videos)

def fuzzy_filter_videos(videos, threshold=80):
    filtered_videos = []
    
    for _, video in videos.iterrows():
        title = video['title'].lower()
        best_match = None
        best_score = 0
        
        for game, keywords in video_filter.items():
            # Check each keyword against the title
            for keyword in keywords:
                score = fuzz.partial_ratio(keyword.lower(), title)
                if score > threshold and score > best_score:
                    best_score = score
                    best_match = game
            print(f'Best Score for {title} : {best_score}')
        
        if best_match:
            video['game'] = best_match
            print(f'Found match: {best_match} with score: {best_score} for video: {title}')
            filtered_videos.append(video)
    
    return pd.DataFrame(filtered_videos)

def token_filter_videos(videos: pd.DataFrame, threshold=80): 
    for _, video in videos.iterrows(): 
        title = video['title'].lower()


if __name__ == "__main__":
    # main()
    user_input = input("Would you like to run main or testbedmain? (m/t): ")
    if user_input.lower() == 'm':
        main()
    elif user_input.lower() == 't':
        testBedMain()
    else:
        print("Invalid input. Please enter 'm' for main or 't' for testbedmain.")
        exit(1)