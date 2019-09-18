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

from create_web_data import get_months

def process_social_referral_df(df):
    ts_df = pd.melt(df, id_vars=["uid", "month"], value_vars = [col for col in df.columns if "ts" in col])
    ctr_df = pd.melt(df, id_vars=["uid", "month"], value_vars = [col for col in df.columns if "ctr" in col]) 
    ts_df["platform"] = np.vectorize(lambda x: x.split()[0])(ts_df["variable"])
    ts_df = ts_df.rename({'value' : 'ts'}, axis='columns')
    ctr_df = ctr_df.rename({'value' : 'ctr'}, axis='columns')
    ctr_df["platform"] = np.vectorize(lambda x: x.split()[0])(ts_df["variable"])
    df_all = ts_df[["uid", "month", "platform", "ts"]].merge(ctr_df[["uid", "month", "platform", "ctr"]], on =["uid", "month", "platform"], how = "left")
    return df_all

if __name__ == "__main__":
    dfs = []
    # Again, in reality data is partioned by month but not for sample
    for month in get_months(201601, 201812):
        referrals = pd.read_csv("data/referral_dummy_data.csv", index_col = None)
        grouped = referrals.groupby(["uid", "origin", "media_label"])["domain"].count().reset_index()
        grouped = grouped.pivot_table(index=["uid", "origin"], columns=["media_label"], values = "domain")
        grouped = grouped.fillna(0.0)
        grouped["news_ref"] = (grouped["fake"] + grouped["msm"])
        grouped["all_ref"] =  (grouped["fake"] + grouped["msm"] + grouped["other"])
        grouped = grouped[["news_ref", "all_ref"]].fillna(0.0).reset_index()
        grouped.columns=["uid", "platform", "news_ref", "all_ref"]
        grouped["platform"] = np.vectorize(lambda s: s.replace(".com", ""))(grouped["platform"])
        df2 = pd.read_csv("data/social_ts_dummy_data.csv")
        df2 = process_social_referral_df(df2)
        df2 = df2[df2["ts"] > 0]
        final = df2.merge(grouped, on =["uid", "platform"], how= "left").fillna(0.0)
        final["month"] = month
        dfs.append(final)
    full_df = pd.concat(dfs)
    full_df.to_csv("data/social_media_compare_dummy.csv", index = False)




