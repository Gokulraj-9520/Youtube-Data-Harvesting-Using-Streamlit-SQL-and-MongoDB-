from googleapiclient.discovery import build
from pprint import pprint
API_KEY = "AIzaSyBzuWfVw8zP9U6cY7YvgvbDai4IMcL7bGc" 
youtube = build("youtube", "v3", developerKey=API_KEY)
#channel_id="UCnz-ZXXER4jOvuED5trXfEA"
def get_channel_info(channel_id):
    response = youtube.channels().list(
        part="snippet,statistics,status,contentDetails", 
        id=channel_id
        ).execute()
    for value in response['items']:
        id_=value['id']
        title=value['snippet']['title']
        description=value['snippet']['description']
        subscriber_count=value['statistics']['subscriberCount']
        status=value['status']['privacyStatus']
        view_count=value['statistics']['viewCount']
        videos_count=value['statistics']['videoCount']
        upload_id= value['contentDetails']['relatedPlaylists']['uploads']
    channel_info={"channel_id":id_,
                  "channel_name":title,
                  "channel_description":description,
                  "channel_subscriber_count":subscriber_count,
                  "channel_view_count":view_count,
                  "channel_videos_count":videos_count,
                  "channel_status":status,
                  "channel_upload_id":upload_id}
    return channel_info
def parse_duration(duration):
    duration=duration[2:]
    hours=0
    minutes=0
    seconds=0
    if 'H' in duration:
        hours_part,duration=duration.split('H')
        hours=int(hours_part)
    if 'M' in duration:
        minutes_part,duration=duration.split('M')
        minutes=int(minutes_part)
    if 'S' in duration:
        seconds=int(duration[:-1])
    total_duration=f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return total_duration
#channel_info = get_channel_info(channel_id)
'''
if channel_info:
    print("Channel Information :")
    print(f"Channel ID: {channel_info['channel_id']}")
    print(f"Channel Name: {channel_info['channel_name']}")
    print(f"Description: {channel_info['channel_description']}")
    print(f"Subscriber Count: {channel_info['channel_subscriber_count']}")
    print(f"View Count: {channel_info['channel_view_count']}")
    print(f"Videos Count: {channel_info['channel_videos_count']}")
    print(f"Channel Status: {channel_info['channel_status']}")
    print(f"Channel Upload ID: {channel_info['channel_upload_id']}")
print("Collect The Information From Video")
print("***********************************")
'''
def get_playlist_info(channel_id):
    next_page_token = None
    playlist_response=[]
    while True:
        response=youtube.playlists().list(
            part='snippet,contentDetails',
        channelId=channel_id,
        maxResults=50,
        pageToken=next_page_token
        ).execute()
        playlist_response.extend(response['items'])
        next_page_token=response.get('nextPageToken')
        if not next_page_token:
            break

    playlist_details=[]
    for item in playlist_response:
        playlist_id=item['id']
        playlist_name=item['snippet']['title']
        playlist_published_date=item['snippet']['publishedAt']
        playlist_description=item['snippet']['description']
        playlist_videoCount=item['contentDetails']['itemCount']
        playlist_details.append({
            "playlist_id":playlist_id,
            "playlist_name":playlist_name,
            "channel_id":channel_id,
            "playlist_published_date":playlist_published_date,
            "playlist_description":playlist_description,
            "playlist_videoCount":playlist_videoCount
                                })
    return playlist_details
#playlist_info=get_playlist_info(channel_id)
#print("********")
def get_video_info(playlist_info):
    video_details=[]
    for playlist in playlist_info:
        playlist_id=playlist['playlist_id']
        playlist_name=playlist['playlist_name']
        video_ids=[]
        next_page_token = None
        while True:
            playlist_items = youtube.playlistItems().list(
                part='snippet,contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            for item in playlist_items['items']:
                video_ids.append(item['snippet']['resourceId']['videoId'])
            next_page_token = playlist_items.get('nextPageToken')
            if not next_page_token:
                break
        for video_id in video_ids:
            next_page_token=None
            while True:
                response=youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=video_id,
                    maxResults=50,
                    pageToken=next_page_token
                ).execute()
                for item in response['items']:
                    video_id=item['id']
                    video_name=item['snippet']['title']
                    channels_id=item['snippet']['channelId']
                    channels_name=item['snippet']['channelTitle']
                    video_description=item['snippet']['description']
                    video_published_date=item['snippet']['publishedAt']
                    video_duration=parse_duration(item['contentDetails']['duration'])
                    video_likes=item['statistics']['likeCount']
                    video_dislikes='0'
                    video_favorite=item['statistics']['favoriteCount']
                    video_views=item['statistics']['viewCount']
                    video_comments_count=item['statistics']['commentCount']
                    video_thumbnails=item['snippet']['thumbnails']['default']['url']
                    try:
                        video_tags=str(item['snippet']['tags'])
                    except:
                        video_tags=''
                    video_caption_status=item['contentDetails']['caption']
                    data={"video_id":video_id,
                          "video_name":video_name,
                          "channels_id":channels_id,
                          "channels_name":channels_name,
                          "playlist_id":playlist_id,
                          "playlist_name":playlist_name,
                          "video_description":video_description,
                          "video_published_date":video_published_date,
                          "video_duration":video_duration,
                          "video_likes":video_likes,
                          "video_dislikes":video_dislikes,
                          "video_favorite":video_favorite,
                          "video_views":video_views,
                          "video_comments_count":video_comments_count,
                          "video_thumbnails":video_thumbnails,
                          "video_tags":video_tags,
                          "video_caption_status":video_caption_status
                          }
                    video_details.append(data)
                next_page_token=response.get('nextPageToken')
                if not next_page_token:
                    break
    return video_details
#videos_info=get_video_info(playlist_info)
def get_comment_info(videos_info):
    comment_details=[]
    comment_count=0
    for videos in videos_info:
        video_id=videos['video_id']
        video_name=videos['video_name']
        comment_id=[]
        next_page_token = None
        while True:
            comment_items=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            x=0
            for comment_thread in comment_items['items']:
                comment_id=comment_thread['id']
                video_id=video_id
                video_name=video_name
                comment_text=comment_thread['snippet']['topLevelComment']['snippet']['textDisplay']
                comment_published_date=comment_thread['snippet']['topLevelComment']['snippet']['publishedAt']
                author_name=comment_thread['snippet']['topLevelComment']['snippet']['authorDisplayName']
                comment_likes_count=comment_thread['snippet']['topLevelComment']['snippet']['likeCount']
                comment_details.append({"comment_id":comment_id,
                                        "video_id": video_id,
                                        "video_name":video_name,
                                        "comment_text":comment_text,
                                        "comment_published_date":comment_published_date,
                                        "author_name":author_name,
                                        "comment_likes_count":comment_likes_count
                                        })
                x+=1
                #print(x)
                if x>=100:
                    break
            next_page_token=comment_items.get('nextPageToken')
            if not next_page_token:
                break
    return comment_details
#comment_details =get_comment_info(videos_info)
#data={"channel_info":channel_info,"playlist_info":playlist_info,"videos_info":videos_info,"comment_details":comment_details}
import json
#output_file_path = f'{ channel_id }.json'

# Save the data to a JSON file
#with open(output_file_path, 'w', ) as json_file:
#    json.dump(data, json_file,  indent=4)
