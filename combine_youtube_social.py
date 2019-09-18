import pandas as pd
import csv
import numpy as np

if __name__ == "__main__":
	df = pd.read_csv("social_media_compare_dummy.csv", index_col = None)
	df["active_user"] = df["all_ref"] > 0
	df["news_user"] = (df["news_ref"] > 0).astype(int)
	df = df.groupby(["platform", "month"]).agg({'active_user' : sum, 'news_user' : sum})
	df.fillna(0.0)
	youtube = pd.read_csv("youtube_news_output_dummy.csv")
	youtube.columns = ["month", "total_user", "active_user", "news_user", "percent_news"]
	youtube["platform"] = "youtube"
	youtube = youtube[["platform", "month", "active_user", "news_user"]].set_index(["platform", "month"])
	df = pd.concat([df, youtube])
	df["perc_news_users"] = df["news_user"] / df["active_user"]
	df = df.reset_index()
	df["month"] = df["month"].astype(str)
	df["date"] = pd.to_datetime(df["month"], format="%Y%m")
	df.to_csv("youtube_social_media_active_users.csv", index = False)