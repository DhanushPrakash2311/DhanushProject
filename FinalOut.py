import pymongo

api= "AIzaSyBVo9e6fnknVnPBGfdXGOWG6e9-zYYFd5M"
# c_id= "UCuI5XcJYynHa5k_lqDzAgwQ"
#*********************************************#MONGODB UPLOADING*********************************************************
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_Data"]
mycol=db["data"]

import pymysql
import googleapiclient.discovery
from pprint import pprint
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import re
import streamlit as st
import time
import pymongo
import plotly.express as px
#***********************************************#SQL CONNECTION*********************************************************
mycon =pymysql.connect(host="127.0.0.1",user="root",password="dhanushd")
mycursor = mycon.cursor()
mycursor.execute(f"CREATE DATABASE IF NOT EXISTS youtube;")

st.title("Youtube_Data_Harvesting")
c_id = st.text_input("Valid_ChannelID")


youtube = googleapiclient.discovery.build("youtube","v3",developerKey=api)
def channel_details(channel_id):
  request = youtube.channels().list(
          part="snippet,contentDetails,statistics",
          id= channel_id
      )
  response = request.execute()
  # print(response)
  curr_chan={}
  channel_id = channel_id
  channel_name = response['items'][0]['snippet']['title']
  vid_count = response['items'][0]['statistics']['videoCount']
  subscription_count = response['items'][0]['statistics']['subscriberCount']
  view_count = response['items'][0]['statistics']['viewCount']
  channel_description = response['items'][0]['snippet']['description']
  uploads_pid =  response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  Channel_data = {'Channel_name': {
          "Channel_Name": channel_name,
          "Channel_Id": channel_id ,
          "Video_count": vid_count,
          "Subscription_Count": subscription_count,
          "Channel_Views": view_count,
          "Channel_Description": channel_description,
          "Playlist_Id": uploads_pid}
  }
  # pprint(Channel_data)
  curr_chan.update(Channel_data)
  # pprint(curr_chan)
  video_id = []
  playlist_response = (youtube.playlistItems().list(part='snippet', playlistId= (Channel_data['Channel_name']["Playlist_Id"]))).execute()
  for i in range(len(playlist_response['items'])):
    video_id.append(playlist_response['items'][i]['snippet']['resourceId']['videoId'])

  # print(video_id)

  for i in range(len(video_id)):
    k = i +1
    request = youtube.videos().list(part="snippet,contentDetails,statistics",id=video_id[i])
    response = request.execute()
    # pprint(response,sort_dicts=0)
    Vid_id = response['items'][0]['id']
    Video_Name = response['items'][0]['snippet']['title']
    Video_Description = response['items'][0]['snippet']['description']
    Tags = response['items'][0]['snippet']['tags']
    PublishedAt = response['items'][0]['snippet']['publishedAt']
    View_Count = response['items'][0]['statistics']['viewCount']
    Like_Count = response['items'][0]['statistics']['likeCount']
    Favorite_Count = response['items'][0]['statistics']['favoriteCount']
    Comment_Count = response['items'][0]['statistics']['commentCount']
    Duration = response['items'][0]['contentDetails']['duration']
    Thumbnail =  response['items'][0]['snippet']['thumbnails']['default']['url']
    Caption_Status ="Available" if response['items'][0]['contentDetails']['caption'] else "Not Available"
    Video_data = {fr"video_id_{k}":{"Video_Id": Vid_id,
            "Video_Name": Video_Name,
            "Video_Description": Video_Description,
            "Tags": Tags,
            "PublishedAt": PublishedAt,
            "View_Count": View_Count,
            "Like_Count": Like_Count,
            "Favorite_Count": Favorite_Count,
            "Comment_Count": Comment_Count,
            "Duration": Duration,
            "Thumbnail": Thumbnail,
            "Caption_Status": Caption_Status,
            "comments": {}}}
    # pprint(Video_data)
    request = youtube.commentThreads().list(part="snippet,replies",videoId=video_id[i])
    response = request.execute()
    for j in range(len(response['items'])):
      Comment_Id = response['items'][j]['snippet']['topLevelComment']['id']
      Comment_Text = response['items'][j]['snippet']['topLevelComment']['snippet']['textOriginal']
      Comment_Author = response['items'][j]['snippet']['topLevelComment']['snippet']['authorDisplayName']
      Comment_PublishedAt = response['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt']
      l=j+1
      cmt={fr"Comment_Id_{l}":{"Comment_Id": Comment_Id,
                      "Comment_Text": Comment_Text,
                      "Comment_Author": Comment_Author,
                      "Comment_PublishedAt": Comment_PublishedAt
                  }}

      Video_data[fr"video_id_{k}"]["comments"].update(cmt)
    curr_chan.update(Video_data)
  mycol.insert_one(curr_chan)
  return curr_chan
  #pprint(curr_chan,sort_dicts=0)

def sql_conv(result):
    #block of code to create dataframe for channel data
    ch_list=[]
    ch_sql = {"Channel_Id": result['Channel_name']['Channel_Id'],
                   "channel_name": result['Channel_name']['Channel_Name'],
              "Video_Count": result['Channel_name']['Video_count'],
              "Subscriber_Count": result['Channel_name']["Subscription_Count"],
                   "Channel_Views": result['Channel_name']['Channel_Views'],
                   "Channel_Description": result['Channel_name']['Channel_Description']}
    ch_list.append(ch_sql)
    ch_df = pd.DataFrame(ch_list)
    #block of code to create dataframe for playlist data
    pl_list=[]
    pl_sql = {"Playlist_Id": result['Channel_name']['Playlist_Id'],
                         "Channel_Id": result['Channel_name']['Channel_Id']}
    pl_list.append(pl_sql)
    pl_df= pd.DataFrame(pl_list)
    #block of code to create dataframe for video data
    cmt_sql_list = []
    vid_sql_list=[]
    for i in range(1, len(result) - 1):

        vid_sql = {"Video_Id": result[f"video_id_{i}"]['Video_Id'],
                   "Playlist_id":result['Channel_name']['Playlist_Id'],
                        "Video_Name": result[f"video_id_{i}"]['Video_Name'],
                        "Video_Description": result[f"video_id_{i}"]['Video_Description'],
                        "PublishedAt": result[f"video_id_{i}"]['PublishedAt'],
                        "View_Count": result[f"video_id_{i}"]['View_Count'],
                        "Like_Count": result[f"video_id_{i}"]['Like_Count'],
                        "Favorite_Count": result[f"video_id_{i}"]['Favorite_Count'],
                        "Comment_Count": result[f"video_id_{i}"]['Comment_Count'],
                        "Duration": result[f"video_id_{i}"]['Duration'],
                        "Thumbnail": result[f"video_id_{i}"]['Thumbnail'],
                        "Caption_Status": result[f"video_id_{i}"]['Caption_Status']}
        vid_sql_list.append(vid_sql)
        video_df = pd.DataFrame(vid_sql_list)
        #block of code to create dataframe for comment data
        for j in range(1,len(result[f"video_id_{i}"]['comments'])):
            comment_data_sql = {"Comment_Id": result[f"video_id_{i}"]['comments'][f"Comment_Id_{j}"]['Comment_Id'],
                                "Video_Id": result[f"video_id_{i}"]['Video_Id'],
                                "Comment_Text": result[f"video_id_{i}"]['comments'][f"Comment_Id_{j}"]['Comment_Text'],
                                "Comment_Author": result[f"video_id_{i}"]['comments'][f"Comment_Id_{j}"]['Comment_Author'],
                                "Comment_PublishedAt": result[f"video_id_{i}"]['comments'][f"Comment_Id_{j}"]['Comment_PublishedAt']}
            cmt_sql_list.append(comment_data_sql)
            Comments_df = pd.DataFrame(cmt_sql_list)

    return {'A':ch_df,'B':pl_df,'C':video_df,'D':Comments_df}

#channel_details("UCuI5XcJYynHa5k_lqDzAgwQ")

#function to declare datatype and covert df to sql table
def table(df):
    #accessing the sql conv function
    channel_df =df['A']
    video_df = df['C']
    Comments_df = df['D']
    playlist_df = df['B']
       #creating a engine using sqlalchemy for datatype decalaration
    engine = create_engine('mysql+pymysql://root:dhanushd@localhost/youtube', echo=False)
    #converting channel dataframe to sql table channel
    channel_df.to_sql('channel', engine, if_exists='append', index=False,
                      dtype={"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                             "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                             "Video_count": sqlalchemy.types.INT,
                             "Subscriber_Count": sqlalchemy.types.BigInteger,
                             "Channel_Views": sqlalchemy.types.BigInteger,
                             "Channel_Description": sqlalchemy.types.TEXT,
                             "Playlist_Id": sqlalchemy.types.VARCHAR(length=225), })
       #converting playlist dataframe to sql table playlist
    playlist_df.to_sql('playlist', engine, if_exists='append', index=False,
                       dtype={"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                              "Playlist_Id": sqlalchemy.types.VARCHAR(length=225), })
       #converting video dataframe to sql table video
    video_df.to_sql('video', engine, if_exists='append', index=False,
                    dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                           'Playlist_Id': sqlalchemy.types.VARCHAR(length=225),
                           'Video_Name': sqlalchemy.types.VARCHAR(length=225),
                           'Video_Description': sqlalchemy.types.TEXT,
                           'Published_date': sqlalchemy.types.String(length=50),
                           'View_Count': sqlalchemy.types.BigInteger,
                           'Like_Count': sqlalchemy.types.BigInteger,
                           'Favorite_Count': sqlalchemy.types.INT,
                           'Comment_Count': sqlalchemy.types.INT,
                           'Duration': sqlalchemy.types.VARCHAR(length=1024),
                           'Thumbnail': sqlalchemy.types.VARCHAR(length=225),
                           'Caption_Status': sqlalchemy.types.VARCHAR(length=225), })
       #converting comments dataframe to sql table comments
    Comments_df.to_sql('comments', engine, if_exists='append', index=False,
                       dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                              'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                              'Comment_Text': sqlalchemy.types.TEXT,
                              'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                              'Comment_Published_date': sqlalchemy.types.String(length=50), })


def analysis():
    st.title("DATA ANALYSIS")
    # queries have been listed using a selectbox
    queries = st.selectbox('**Select your Question**',
                           ('1. What are the names of all the videos and their corresponding channels?',
                            '2. Which channels have the most number of videos, and how many videos do they have?',
                            '3. What are the top 10 most viewed videos and their respective channels?',
                            '4. How many comments were made on each video, and what are their corresponding video names?',
                            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                            '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                            '8. What are the names of all the channels that have published videos in the year 2022?',
                            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                            '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),
                           key='collection_question')
    mycon = pymysql.connect(host="127.0.0.1", user="root", password="dhanushd", database='youtube')
    mycursor = mycon.cursor()
    # block of code to analyse 1st query
    if queries == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute(
            "SELECT channel.Channel_Name, video.Video_Name FROM channel JOIN playlist JOIN video ON channel.Channel_Id = playlist.Channel_Id AND playlist.Playlist_Id = video.Playlist_Id;")
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)
    # block of code to analyse 2nd query
    elif queries == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("SELECT Channel_Name, Video_Count FROM channel ORDER BY Video_Count DESC;")
        result_2 = mycursor.fetchall()
        df2 = pd.DataFrame(result_2, columns=['Channel Name', 'Video Count']).reset_index(drop=True)
        df2.index += 1
        st.dataframe(df2)
    # block of code to analyse 3rd query
    elif queries == '3. What are the top 10 most viewed videos and their respective channels?':
        col1, col2 = st.columns(2)
        with col1:
            mycursor.execute(
                "SELECT channel.Channel_Name, video.Video_Name, video.View_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.View_Count DESC LIMIT 10;")
            result_3 = mycursor.fetchall()
            df3 = pd.DataFrame(result_3, columns=['Channel Name', 'Video Name', 'View count']).reset_index(
                drop=True)
            df3.index += 1
            st.dataframe(df3)

        with col2:
            fig_topvc = px.bar(df3, y='View count', x='Video Name', text_auto='.2s',
                               title="Top 10 most viewed videos")
            fig_topvc.update_traces(textfont_size=18, marker_color='#F3ff5c')
            fig_topvc.update_layout(title_font_color='#5cfff0 ', title_font=dict(size=30))
            st.plotly_chart(fig_topvc, use_container_width=True)
    # block of code to analyse 4th query
    elif queries == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute(
            "SELECT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id;")
        result_4 = mycursor.fetchall()
        df4 = pd.DataFrame(result_4, columns=['Channel Name', 'Video Name', 'Comment count']).reset_index(drop=True)
        df4.index += 1
        st.dataframe(df4)

    # block of code to analyse 5th query
    elif queries == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute(
            "SELECT channel.Channel_Name, video.Video_Name, video.Like_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
        result_5 = mycursor.fetchall()
        df5 = pd.DataFrame(result_5, columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
        df5.index += 1
        st.dataframe(df5)
    # block of code to analyse 6th query
    elif queries == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
        mycursor.execute(
            "SELECT channel.Channel_Name, video.Video_Name, video.Like_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
        result_6 = mycursor.fetchall()
        df6 = pd.DataFrame(result_6, columns=['Channel Name', 'Video Name', 'Like count']).reset_index(
            drop=True)
        df6.index += 1
        st.dataframe(df6)
    # block of code to analyse 7th query
    elif queries == '7. What is the total number of views for each channel, and what are their corresponding channel names?':

        col1, col2 = st.columns(2)
        with col1:
            mycursor.execute("SELECT Channel_Name, Channel_Views FROM channel ORDER BY Channel_Views DESC;")
            result_7 = mycursor.fetchall()
            df7 = pd.DataFrame(result_7, columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
            df7.index += 1
            st.dataframe(df7)

        with col2:
            fig_topview = px.bar(df7, y='Total number of views', x='Channel Name', text_auto='.2s',
                                  title="Total number of views", )
            fig_topview.update_traces(textfont_size=18, marker_color='#F3ff5c')
            fig_topview.update_layout(title_font_color='#5cfff0 ', title_font=dict(size=30))
            st.plotly_chart(fig_topview, use_container_width=True)
    # block of code to analyse 8th query
    elif queries == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute(
            "SELECT channel.Channel_Name, video.Video_Name, video.PublishedAt FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id  WHERE EXTRACT(YEAR FROM PublishedAt) = 2022;")
        result_8 = mycursor.fetchall()
        df8 = pd.DataFrame(result_8, columns=['Channel Name', 'Video Name', 'Year 2022 only']).reset_index(
            drop=True)
        df8.index += 1
        st.dataframe(df8)
    # block of code to analyse 9th query
    elif queries == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute(
            "SELECT channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS duration  FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC ;")
        result_9 = mycursor.fetchall()
        df9 = pd.DataFrame(result_9, columns=['Channel Name', 'Average duration of videos (HH:MM:SS)']).reset_index(
            drop=True)
        df9.index += 1
        st.dataframe(df9)
    # block of code to analyse 10th query
    elif queries == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute(
            "SELECT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Comment_Count DESC;")
        result_10 = mycursor.fetchall()
        df10 = pd.DataFrame(result_10, columns=['Channel Name', 'Video Name', 'Number of comments']).reset_index(
            drop=True)
        df10.index += 1
        st.dataframe(df10)

if st.button("Run"):
    df = channel_details(c_id)
    abc = sql_conv(df)
    table(abc)
if st.checkbox("JSON Data"):
    mongo = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = mongo["Youtube_Data"]
    mycollection = mydb['data']
    dbcursor = mycollection.find({})
    for document in dbcursor:
        st.write(document)

mycon = pymysql.connect(host="127.0.0.1", user="root", password="dhanushd",database='youtube')
mycursor = mycon.cursor()
#block of code to display table channel
if st.checkbox("Channel Data"):
    st.subheader("TABLE_CHANNEL")
    mycursor.execute('SELECT * FROM channel;')
    ch_data = mycursor.fetchall()
    ch_df = pd.DataFrame(ch_data, columns=[i[0] for i in mycursor.description])
    st.dataframe(ch_df)
#block of code to display table playlist
if st.checkbox("Playlist Data"):
    st.subheader("TABLE_PLAYLIST")
    mycursor.execute('SELECT * FROM playlist;')
    pl_data = mycursor.fetchall()
    pl_df = pd.DataFrame(pl_data, columns=[i[0] for i in mycursor.description])
    st.dataframe(pl_df)
#block of code to display table comments
if st.checkbox("Comment Data"):
    st.subheader("TABLE_COMMENTS")
    mycursor.execute('SELECT * FROM comments;')
    cmt_data = mycursor.fetchall()
    cmt_df = pd.DataFrame(cmt_data, columns=[i[0] for i in mycursor.description])
    st.dataframe(cmt_df)
#block of code to display table video
if st.checkbox("Video Data"):
    st.subheader("TABLE_VIDEO")
    mycursor.execute('SELECT * FROM video;')
    vid_data = mycursor.fetchall()
    vid_df = pd.DataFrame(vid_data, columns=[i[0] for i in mycursor.description])
    st.dataframe(vid_df)
if st.checkbox("Data Analysis"):
    analysis()




