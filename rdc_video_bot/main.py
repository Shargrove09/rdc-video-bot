import os

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from dotenv import load_dotenv
from sheet import set_video_sheet
from datetime import datetime
from rapidfuzz import fuzz, process

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

load_dotenv()

def fetchVideosFromPlaylist(youtube, pageToken=None): 
    # Fetch Playlist Results
    if (youtube): 
        playlistRequest = youtube.playlistItems().list(
            part="snippet,contentDetails",
            maxResults=50, # 50 is max limit set by youtube API 
            playlistId="UUOnECY8FBKKPVi5ZsSgXPJA",
            pageToken=pageToken
        )
        playlist_results = playlistRequest.execute(); 
        global page_token
        print(playlist_results) 
        try: 
            page_token = playlist_results['nextPageToken']
        except: 
            print("\n Couldn't find page token!")
            page_token = ""
        return playlist_results
    else:  
        print("Youtube Client not initialized!")

def parseVideos(playlist_results, video_data_list): 
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
            "video_id": video_id,
            "added_to_db": False,
            "date": formatted_date
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
    page_token = ""
    
    for i in range(25): 
        fetched_videos = fetchVideosFromPlaylist(youtube, page_token)
        parseVideos(fetched_videos, video_data)
        i += 1



    df = pd.DataFrame(video_data)
    filtered_df = fuzzy_filter_videos(df)
    print("--- \n Filtered DF \n --- \n", filtered_df)
    set_video_sheet(filtered_df)




video_filter = { 
    "MK8": ["MK8", "Mario Kart 8", "Mario Kart 8 Deluxe"],
    "COD": ["COD", "Call of Duty", "Call of Duty Warzone", "Call of Duty Black Ops Cold War", "Call of Duty Black Ops 6", "Black Ops 6"],
    "Rocket League": ["Rocket League",],
}

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
        
        if best_match:
            video['game'] = best_match
            video['match_score'] = best_score
            print(f'Found match: {best_match} with score: {best_score} for video: {title}')
            filtered_videos.append(video)
    
    return pd.DataFrame(filtered_videos)


if __name__ == "__main__":
    main()