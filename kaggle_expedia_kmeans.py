# coding: utf-8

"""
	author: Loïc M. DIVAD
	date: 2016-05-12
	see also: https://www.kaggle.com/c/expedia-hotel-recommendations
"""

# imports
import sys
import time
import math
import random
import pandas as pd
import numpy  as np

from sklearn.cluster import KMeans
from sklearn.cross_validation import train_test_split

# contants
SEED_STATE = 1
KFOLD_NUMB = 10
TARGET = "hotel_cluster"
NON_FEATURES = []
LOG_FORMAT = '%(asctime)s [ %(levelname)s ] : %(message)s'

# logging
import logging
import warnings
warnings.filterwarnings("ignore")

logging.basicConfig(format=LOG_FORMAT)
log = logging.getLogger()
log.setLevel(logging.INFO)

def parseTime(sec):
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)

def buildTheTop(df):
    top = {}
    clusters = df["kmeans"].unique()
    for c in clusters:
        c_df = df[(df["kmeans"] == c) & (df["is_booking"] == 1)]
        top["cluster_%s"%c] = c_df.hotel_cluster.value_counts().index

    top = pd.DataFrame( dict([ (k,pd.Series(v)) for k,v in top.iteritems() ]))
    top.fillna(0, inplace=True)
    return top

def expediaPredict(df, top, n=5):
    submition = "%s "*n
    df["list_hotel_cluster"] = df["kmeans"].apply(lambda y: top["cluster_%s"%y][:n].astype(int).tolist())
    df["str_hotel_cluster"]  = df["list_hotel_cluster"].apply(lambda y: submition%tuple(y))
    return df

def dataClean(df):

    df["date_time"] = pd.to_datetime(df["date_time"])
    df["srch_ci"] = pd.to_datetime(df["srch_ci"], format="%Y-%m-%d", errors="coerce")
    df["srch_co"] = pd.to_datetime(df["srch_co"], format="%Y-%m-%d", errors="coerce")

    props = {}
    for prop in ["month", "day", "hour", "minute", "dayofweek", "quarter"]:
        props[prop] = getattr(df["date_time"].dt, prop)
    
    features = [p for p in df.columns if p not in ["date_time", "srch_ci", "srch_co"]]
    
    for prop in features:
        props[prop] = df[prop]
    
    date_props = ["month", "day", "dayofweek", "quarter"]
    for prop in date_props:
        props["ci_{0}".format(prop)] = getattr(df["srch_ci"].dt, prop)
        props["co_{0}".format(prop)] = getattr(df["srch_co"].dt, prop)
    props["stay_span"] = (df["srch_co"] - df["srch_ci"]).astype('timedelta64[h]')
        
    frame = pd.DataFrame(props)
    
    #frame = frame.join(dest_pca, on="srch_destination_id", how='left', rsuffix="dest")
    #frame = frame.drop("srch_destination_iddest", axis=1)    
    
    #frame = frame.drop(toDrop, axis=1)
    orig_dest_dist_median = frame["orig_destination_distance"].median()
    frame["orig_destination_distance"].fillna(orig_dest_dist_median, inplace=True)
    
    return frame


if __name__ == "__main__":

    start_time = time.time()

    log.info("Loading the dataset : train.csv")
    DATA_TRAIN = pd.read_csv("../data/train.csv")

    log.info("Loading the dataset : destinations.csv")
    DATA_DEST = pd.read_csv("../data/destinations.csv")

    log.info("Loading the dataset : test.csv")
    DATA_TEST = pd.read_csv("../data/test.csv")

    selected = DATA_TEST["user_id"].unique()
    DATA_TARGET = DATA_TRAIN[DATA_TRAIN.user_id.isin(selected)]
    DATA_TRAIN, DUMP_TRAIN =  train_test_split(DATA_TARGET, test_size=0.60, random_state=SEED_STATE)

    log.info("Data Cleaning : data.csv and test.csv")
    train = dataClean(DATA_TRAIN)
    test = dataClean(DATA_TEST)

    train.fillna(-1, inplace=True)
    test.fillna(-1, inplace=True)

    log.info("KMEANS ALGORITHM RUNNNING !!! ")
    kmeans = KMeans(n_clusters=40, random_state=SEED_STATE, n_jobs=-1)
    kmeans.fit(train.drop(["user_id", "is_booking", "hotel_cluster"], axis=1), train["hotel_cluster"])

    log.info("Building User Cluster prediction")
    train["kmeans"] = kmeans.predict(train.drop(["user_id", "is_booking", "hotel_cluster"], axis=1))
    test["kmeans"]  = kmeans.predict(test.drop(["user_id"], axis=1))

    log.info("Grouping the top prediction per user cluster")
    theTop = buildTheTop(train)

    log.info("Final prediction")
    result = expediaPredict(test, theTop)

    log.info("Wrinting result ... ")
    result["id"] = range(len(result))
    result.to_csv("../data/loic_prediction.csv", columns=["id", "str_hotel_cluster"], header=["id", "hotel_cluster"], index=False)

    log.info("~ • ~ END OF THE SCRIPT, DURATION: %s Hours ~ • ~"%(parseTime(time.time() - start_time)))

