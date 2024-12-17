import os

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from dotenv import load_dotenv
import gspread


scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
load_dotenv()

def main():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"

    api_key= os.getenv("API_KEY")
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key)
    
    playlistRequest = youtube.playlistItems().list(
        part="snippet,contentDetails",
        maxResults=100,
        playlistId="UUOnECY8FBKKPVi5ZsSgXPJA"
    )


    playlist_results = playlistRequest.execute(); 

    video_data = []
    for video in playlist_results.get('items', []): 
        title = video['snippet']['title']
        video_id = video['contentDetails']['videoId']
        print(f"Title: {title}\nVideo ID: {video_id}\n")
        content_details = video['contentDetails']

        date = content_details['videoPublishedAt']
        # print("\n CD", content_details)
        # print(video)
        video_data.append({
            "title": title,
            "video_id": video_id,
            "added_to_db": False,
            "date": date
        })

    df = pd.DataFrame(video_data)
    print(filter_videos(df))




video_filter = { 
    "MK8": ["MK8", "Mario Kart 8", "Mario Kart 8 Deluxe"],
    "COD": ["COD", "Call of Duty", "Call of Duty Warzone", "Call of Duty Black Ops Cold War", "Call of Duty Black Ops 6", "Black Ops 6"],
    "Rocket League": ["Rocket League", "RL"],
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


if __name__ == "__main__":
    main()