from pprint import pprint
import googleapiclient.discovery
import json
import pymongo
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import sqlalchemy
import mysql.connector
import plotly.express as px
from details import get_channel_info,get_playlist_info,get_video_info,get_comment_info
# Get a Google API key
API_KEY = "AIzaSyBzuWfVw8zP9U6cY7YvgvbDai4IMcL7bGc" #AIzaSyBn_MUZo_MYypIXUU6UA-JwGdmr4RHExeY
# Create a YouTube API service object
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)
# Create a MongoDB client
#client = pymongo.MongoClient("mongodb://localhost:27017/")
client=pymongo.MongoClient("mongodb+srv://Gokulraj:041XHepao1ZUzA34@cluster0.8as3myf.mongodb.net/")
# Extract information on a YouTube channel
db=client['youtube_data']
collection=db['youtube_details']
connection=mysql.connector.connect(host='localhost',user='root',password='admin',database='youtube')
my_cursor=connection.cursor()

query="""create table if not exists channel(channel_id VARCHAR(225) unique,
            channel_name VARCHAR(225),
            channel_description TEXT,
            channel_subscriber_count BIGINT,
            channel_view_count INT,
            channel_videos_count INT,
            Channel_Views BIGINT,
            channel_status VARCHAR(50),
            channel_upload_id VARCHAR(225) )"""

my_cursor.execute(query)
query="""CREATE TABLE IF NOT EXISTS playlist (
            playlist_id VARCHAR(225) unique,
            playlist_name VARCHAR(255),
            channel_id VARCHAR(255),
            playlist_published_date VARCHAR(50),
            playlist_description TEXT,
            playlist_videoCount INT
        );
        """
my_cursor.execute(query)

query="""CREATE TABLE IF NOT EXISTS video (
    video_id VARCHAR(225) UNIQUE,
    video_name VARCHAR(225),
    channels_id VARCHAR(225),
    channels_name VARCHAR(225),
    playlist_Id VARCHAR(225),
    playlist_name VARCHAR(225),
    video_description TEXT,
    video_published_date VARCHAR(50),
    video_duration VARCHAR(1024),
    video_likes BIGINT,
    video_dislikes INT,
    video_favorite INT,
    video_views BIGINT,
    video_comments_count INT,
    video_thumbnails VARCHAR(225),
    video_tags TEXT,
    video_caption_status VARCHAR(225)
);
"""
my_cursor.execute(query)

query=""" CREATE TABLE IF NOT EXISTS comment (
    comment_id VARCHAR(225) UNIQUE,
    video_id VARCHAR(225),
    video_name VARCHAR(225),
    comment_text TEXT,
    comment_published_date VARCHAR(50),
    author_name VARCHAR(225),
    comment_likes_count INT
);
"""
my_cursor.execute(query)

connection.close()
# Build a Streamlit application
def main():
    st.title("YouTube Channel Data Harvesting and Warehousing")
    # Get the channel ID from the user
    channel_id = st.text_input("Enter a YouTube channel ID:")
    if channel_id:
        try:
            channel_info = get_channel_info(channel_id)
            playlist_info=get_playlist_info(channel_id)
            videos_info=get_video_info(playlist_info)
            comment_details =get_comment_info(videos_info)
            data={
                "channel_info":channel_info,
                "playlist_info":playlist_info,
                "videos_info":videos_info,
                "comment_details":comment_details
            }
            #print(data)
            channel_df = pd.DataFrame.from_dict(channel_info, orient='index').T
            playlist_df = pd.DataFrame(playlist_info)
            video_df = pd.DataFrame(videos_info)
            comments_df = pd.DataFrame(comment_details)
            #pprint(playlist_df)
        except:
            st.warning("Today Quota is Complete")
    if st.button('Store to Mongo DB'):
        value=collection.find_one({"channel_info.channel_id":channel_id})
        if value is None:
            collection.insert_one(data)
            st.success("Successfully stored in Mongo DB")
        else:
            st.warning("This channel is already stored in Mongo DB")
    if st.button('Transfer MongoDB to MySQL'):
        document=collection.find_one({'channel_info.channel_id':channel_id})
        if document is None:
            st.warning("This channel is not stored in Mongo DB.")
        else:
            for key,value in document.items():
                if key=='channel_info':
                    channel_info= value
                elif key=='playlist_info':
                    playlist_info=value
                elif key=='videos_info':
                    videos_info=value
                elif key=='comment_details':
                    comment_details=value
            channel_df = pd.DataFrame.from_dict(channel_info, orient='index').T
            playlist_df = pd.DataFrame(playlist_info)
            video_df = pd.DataFrame(videos_info)
            comments_df = pd.DataFrame(comment_details)
            print("Before Remove Duplicates")
            print(playlist_df.shape, video_df.shape,comments_df.shape)
            channel_df= channel_df.drop_duplicates(subset='channel_id', keep='first')
            playlist_df=playlist_df.drop_duplicates(subset='playlist_id',keep='first')
            video_df=video_df.drop_duplicates(subset='video_id', keep='first')
            comments_df=comments_df.drop_duplicates(subset='comment_id', keep='first')
            print("After Remove Duplicates")
            print(playlist_df.shape, video_df.shape,comments_df.shape)
            engine = create_engine('mysql+mysqlconnector://root:admin@localhost/youtube', echo=False)
            try:
                channel_df.to_sql('channel', engine, if_exists='append', index=False,
                                dtype = {
                                        "channel_id": sqlalchemy.types.VARCHAR(length=225),
                                        "channel_name": sqlalchemy.types.VARCHAR(length=225),
                                        "channel_description": sqlalchemy.types.TEXT,
                                        "channel_subscriber_count": sqlalchemy.types.BigInteger,
                                        "channel_view_count":sqlalchemy.types.INT,
                                        "channel_videos_count": sqlalchemy.types.INT,
                                        "Channel_Views": sqlalchemy.types.BigInteger,
                                        "channel_status":sqlalchemy.types.VARCHAR(length=50),
                                        "channel_upload_id":sqlalchemy.types.VARCHAR(length=225)})
                playlist_df.to_sql('playlist', engine, if_exists='append', index=False,
                                dtype = {"playlist_id": sqlalchemy.types.VARCHAR(length=225),
                                        "playlist_name":sqlalchemy.types.VARCHAR(length=255),
                                        "channel_id":sqlalchemy.types.VARCHAR(length=255),
                                        "playlist_published_date":sqlalchemy.types.String(length=50),
                                        "playlist_description":sqlalchemy.types.TEXT,
                                        "playlist_videoCount":sqlalchemy.types.INT
                                        })
                video_df.to_sql('video', engine, if_exists='append', index=False,
                            dtype = {
                                    'video_id': sqlalchemy.types.VARCHAR(length=225),
                                    'video_name': sqlalchemy.types.VARCHAR(length=225),
                                    'channels_id': sqlalchemy.types.VARCHAR(length=225),
                                    'channels_name': sqlalchemy.types.VARCHAR(length=225),
                                    'playlist_Id': sqlalchemy.types.VARCHAR(length=225),
                                    'playlist_name': sqlalchemy.types.VARCHAR(length=225),
                                    'video_description': sqlalchemy.types.TEXT,
                                    'video_published_date': sqlalchemy.types.String(length=50),
                                    'video_duration': sqlalchemy.types.VARCHAR(length=1024),
                                    'video_likes': sqlalchemy.types.BigInteger,
                                    'video_dislikes': sqlalchemy.types.INT,
                                    'video_favorite': sqlalchemy.types.INT,
                                    'video_views': sqlalchemy.types.BigInteger,
                                    'video_comments_count': sqlalchemy.types.INT,
                                    'video_thumbnails': sqlalchemy.types.VARCHAR(length=225),
                                    'video_tags': sqlalchemy.types.TEXT,
                                    'video_caption_status': sqlalchemy.types.VARCHAR(length=225),})
                comments_df.to_sql('comments', engine, if_exists='append', index=False,
                                dtype = {
                                        'comment_id': sqlalchemy.types.VARCHAR(length=225),
                                        'video_id': sqlalchemy.types.VARCHAR(length=225),
                                        'video_name': sqlalchemy.types.VARCHAR(length=225),
                                        'comment_text': sqlalchemy.types.TEXT,
                                        'comment_published_date': sqlalchemy.types.String(length=50),
                                        'author_name': sqlalchemy.types.VARCHAR(length=225),
                                        'comment_likes_count':sqlalchemy.types.INT,
                                        })

                st.success("Successfully Stored in MySQL")
            except IntegrityError:
                st.warning("This Channel Already Stored in MySQL")


def Queries():
    connection=mysql.connector.connect(host='localhost',user='root',password='admin',database='youtube')
    cursor = connection.cursor()
    question_tosql = st.selectbox("",('**Select your Question**',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = 'collection_question')

    if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("SELECT channels_name, video_name FROM video;")
        result_1 = cursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':

        col1,col2 = st.columns(2)
        with col1:
   
            cursor.execute("SELECT channel_name, channel_videos_count AS video_count FROM channel ORDER BY video_count DESC;")
            result_2 = cursor.fetchall()
            df2 = pd.DataFrame(result_2,columns=['Channel Name','Video Count']).reset_index(drop=True)
            df2.index += 1
            st.dataframe(df2)

        with col2:
            fig_vc = px.bar(df2, y='Video Count', x='Channel Name', text_auto='.2s', title="Most number of videos", )
            fig_vc.update_traces(textfont_size=16,marker_color='#E6064A')
            fig_vc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
            st.plotly_chart(fig_vc,use_container_width=True)
    elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':

        col1,col2 = st.columns(2)
        with col1:
            cursor.execute("SELECT channel.channel_name, video.video_name, video.video_views FROM channel JOIN video ON channel.channel_id = video.channels_id ORDER BY video.video_views DESC LIMIT 10;")
            result_3 = cursor.fetchall()
            df3 = pd.DataFrame(result_3,columns=['Channel Name', 'Video Name', 'View count']).reset_index(drop=True)
            df3.index += 1
            st.dataframe(df3)

        with col2:
            fig_topvc = px.bar(df3, y='View count', x='Video Name', text_auto='.2s', title="Top 10 most viewed videos")
            fig_topvc.update_traces(textfont_size=16,marker_color='#E6064A')
            fig_topvc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
            st.plotly_chart(fig_topvc,use_container_width=True)

    # Q4 
    elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("SELECT channels_name, video_name, video_comments_count FROM video;")
        result_4 = cursor.fetchall()
        df4 = pd.DataFrame(result_4,columns=['Channel Name', 'Video Name', 'Comment count']).reset_index(drop=True)
        df4.index += 1
        st.dataframe(df4)

    # Q5
    elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("SELECT channels_name, video_name, video_likes FROM video ORDER BY video_likes DESC;")
        result_5= cursor.fetchall()
        df5 = pd.DataFrame(result_5,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
        df5.index += 1
        st.dataframe(df5)

    # Q6
    elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.write('**Note:- YouTube removed the public dislike count from all of the videos.**')
        cursor.execute("SELECT video_name, video_likes, video_dislikes FROM video ORDER BY video_likes DESC;")
        result_6= cursor.fetchall()
        df6 = pd.DataFrame(result_6,columns=['Video Name', 'Like count','Dislike count']).reset_index(drop=True)
        df6.index += 1
        st.dataframe(df6)

    # Q7
    elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':

        col1, col2 = st.columns(2)
        with col1:
            cursor.execute("SELECT channel_name, channel_view_count FROM channel ORDER BY channel_view_count DESC;")
            result_7= cursor.fetchall()
            df7 = pd.DataFrame(result_7,columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
            df7.index += 1
            st.dataframe(df7)
        
        with col2:
            fig_topview = px.bar(df7, y='Total number of views', x='Channel Name', text_auto='.2s', title="Total number of views", )
            fig_topview.update_traces(textfont_size=16,marker_color='#E6064A')
            fig_topview.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
            st.plotly_chart(fig_topview,use_container_width=True)

    # Q8
    elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("SELECT channels_name, video_name, video_published_date FROM video WHERE EXTRACT(YEAR FROM video_published_date) = 2022;")
        result_8= cursor.fetchall()
        df8 = pd.DataFrame(result_8,columns=['Channel Name','Video Name', 'Year 2022 only']).reset_index(drop=True)
        df8.index += 1
        st.dataframe(df8)

    # Q9
    elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel.channel_name, 
                       TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(video.video_duration))), '%H:%i:%s') AS duration
                       FROM channel JOIN playlist ON channel.channel_id = playlist.channel_id JOIN video 
                       ON playlist.playlist_id = video.playlist_id GROUP BY channel.channel_name
                       ORDER BY duration DESC;""")
        result_9= cursor.fetchall()
        df9 = pd.DataFrame(result_9,columns=['Channel Name','Average duration of videos (HH:MM:SS)']).reset_index(drop=True)
        df9.index += 1
        st.dataframe(df9)

    # Q10
    elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("SELECT channels_name, video_name, video_comments_count FROM video ORDER BY video_comments_count DESC;")
        result_10= cursor.fetchall()
        df10 = pd.DataFrame(result_10,columns=['Channel Name','Video Name', 'Number of comments']).reset_index(drop=True)
        df10.index += 1
        st.dataframe(df10)

    # SQL DB connection close
    connection.close()
if __name__ == "__main__":
    background_color_style = """
    <style>
    body {
        background-color: #F0F0F0; /* Replace with your desired color code */
    }
    </style>
    """

    # Use st.markdown to inject the custom CSS
    st.markdown(background_color_style, unsafe_allow_html=True)
    main()
    Queries()
