from glob import glob
import sys
import pandas as pd
import argparse
import numpy as np
import csv
from datetime import datetime
from urllib.parse import urlparse
import time
import os
import gc
import sys
from urllib.parse import urlparse, parse_qs

from create_web_data import get_months


category_dict = {
    "1" : "Film & Animation", \
    "2" : "Autos & Vehicles", \
    "10" : "Music", \
    "15" : "Pets & Animals", \
    "17" : "Sports", \
    "18" : "Short Movies", \
    "19" : "Travel & Events", \
    "20" : "Gaming", \
    "21" : "Videoblogging", \
    "22" : "People & Blogs", \
    "23" : "Comedy", \
    "24" : "Entertainment", \
    "25" : "News & Politics", \
    "26" : "Howto & Style", \
    "27" : "Education", \
    "28" : "Science & Technology", \
    "29" : "Nonprofits & Activism", \
    "30" : "Movies", \
    "31" : "Anime/Animation", \
    "32" : "Action/Adventure", \
    "33" : "Classics", \
    "34" : "Comedy", \
    "35" : "Documentary", \
    "36" : "Drama", \
    "37" : "Family", \
    "38" : "Foreign", \
    "39" : "Horror", \
    "40" : "Sci-Fi/Fantasy", \
    "41" : "Thriller", \
    "42" : "Shorts", \
    "43" : "Shows", \
    "44" : "Trailers", \
}


def video_id(value):
    """
    Examples:
    - http://youtu.be/SA2iWivDJiE
    - http://www.youtube.com/watch?v=_oPAwA_Udwc&feature=feedu
    - http://www.youtube.com/embed/SA2iWivDJiE
    - http://www.youtube.com/v/SA2iWivDJiE?version=3&amp;hl=en_US
    """
    try:
        query = urlparse(value)
    except Exception as e:
        return None
    if query.hostname == 'youtu.be':
        return query.path[1:]
    if query.hostname in ('www.youtube.com', 'youtube.com'):
        if query.path == '/watch':
            p = parse_qs(query.query)
            if 'v' not in p:
                return None
            return p['v'][0]
        if query.path[:7] == '/embed/':
            return query.path.split('/')[2]
        if query.path[:3] == '/v/':
            return query.path.split('/')[2]
    # fail?
    return None

def create_single_row(row):
    row = row.split('|')
    return row[0:11] + [(('').join(row[11:])).strip()]

def process_dataset_by_row(file):
    with open(file, "r", encoding="utf8") as f:
        for idx, line in enumerate(f.readlines()):
            if idx % 10**7 == 0: 
                print("Wrote {}# of rows...".format(idx))
            yield create_single_row(line)

def get_videos(f, users):
    user_videos = []
    for line in process_dataset_by_row(f):
        user = line[1]
        if user not in users:
           continue
        vid_id = video_id(line[11])
        if vid_id == None:
            continue
        else:
            user_videos.append([user, vid_id])
    return user_videos


if __name__ == "__main__":
    with open("data/youtube_news_output_dummy.csv", "w+")  as f:
        writer = csv.writer(f)
        writer.writerow(["month", "total_users", "youtube_users", "news_users", "percent_news"])

        # In reality have data partitioned by month, not true for dummy data
        for month in get_months(201601, 201812):
             #inputfiles
            active_youtube_users = "data/sample_users_dummy.csv"
            user_videos = "data/user_id_video_id_dummy_data.csv"
            videos = "data/video_ids_cat_data.csv"
            df_user = pd.read_csv(active_youtube_users, names =[ "uid"])
            df_video = pd.read_csv(user_videos, names = ["uid", "vid_id"])
            df_cat = pd.read_csv(videos, delimiter = "\t", names =["vid_id", "title", "category", "channel_name", "channel_id"])
            full = df_video.merge(df_cat[["vid_id", "category"]], on = "vid_id", how = "left").fillna(0.0)
            full["is_news"] = (full["category"].astype(float) == 25.0).astype(int)
            full = full.groupby("uid").aggregate({"is_news" : "sum", "category" : "count"}).reset_index()
            full["is_news_1"] = (full["is_news"] > 0).astype(int)
            full["category"] = (full["category"] > 0).astype(int)
            full = df_user.merge(full, on ="uid", how = "left").fillna(0)
            perc = (full["is_news_1"].sum() / full["category"].sum()) * 100
            writer.writerow([month,full["uid"].count(), full["category"].sum(), full["is_news_1"].sum(), perc])
   
