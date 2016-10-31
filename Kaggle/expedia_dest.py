# coding: utf-8

"""
	author: Loïc M. DIVAD
	date: 2016-05-14
	see also: https://www.kaggle.com/c/expedia-hotel-recommendations
"""

# imports
import sys
import time
import math
import random
import operator
import pandas as pd
import numpy  as np

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

def scoring(df):
    topagg = {}
    groups = df.groupby(["srch_destination_id", "hotel_cluster"])

    # building score dict
    for name, group in groups:
        dest_id = name[0]
        cluster_id = name[1]

        clicks = len(group.is_booking[group.is_booking == False])
        bookings = len(group.is_booking[group.is_booking == True])

        score = bookings + .15 * clicks

        if dest_id not in topagg.keys():
            topagg[dest_id] = {}

        topagg[dest_id][cluster_id] = score

    # building the score dataframe
    df_top_col=[]
    for dest_id in sorted(topagg.keys()):
        df_top_col.append([i[0] for i in sorted(topagg[dest_id].items(), key=operator.itemgetter(1), reverse=True)])

    return pd.DataFrame({"srch_destination_id": sorted(topagg.keys()), "list_hotel_cluster":df_top_col})


def predict():
    pass

if __name__ == "__main__":

    start_time = time.time()

    log.info("Loading the dataset > train.csv")
    DATA_TRAIN = pd.read_csv("../data/train.csv")

    log.info("Loading the dataset > destinations.csv")
    DATA_DEST = pd.read_csv("../data/destinations.csv")

    log.info("Loading the dataset > test.csv")
    DATA_TEST = pd.read_csv("../data/test.csv")

    log.info("Filtering the dataset with existing user id from the test set")
    selected = DATA_TEST["user_id"].unique()
    DATA_TRAIN = DATA_TRAIN[DATA_TRAIN.user_id.isin(selected)]
    #DATA_TRAIN, DUMP_TRAIN =  train_test_split(DATA_TARGET, test_size=0.60, random_state=SEED_STATE)

    log.info("Building the GLOBAL_RANK_HOTEL list")
    GLOBAL_RANK_HOTEL = DATA_TRAIN["hotel_cluster"].value_counts().index.tolist()
    log.info("GLOBAL_RANK_HOTEL = %s"%GLOBAL_RANK_HOTEL[:5])

    log.info("Scroring in process ... ")
    topAggDf = scoring(DATA_TRAIN)

    def complet(x):
    	n = 0
    	while len(x) < 5:
    		hc = GLOBAL_RANK_HOTEL[n]
    		n = n + 1
    		if hc not in x:
    			x.append(hc)
    	return x

    def reformat(x):
    	pattern = "%s "*5
    	return pattern%tuple(x[:5])

    
    log.info("Complete and reformat the output")
    topAggDf["list_hotel_cluster"] = topAggDf["list_hotel_cluster"].apply(complet)
    topAggDf["str_hotel_cluster"]  = topAggDf["list_hotel_cluster"].apply(reformat)

    log.info("Join the prediction with the input")
    result = DATA_TEST.join(topAggDf, on="srch_destination_id", how="left", rsuffix="_")
    result["str_hotel_cluster"].fillna(5*"%s "%tuple(GLOBAL_RANK_HOTEL[:5]), inplace=True)

    log.info("Wrinting result ... ")
    result.to_csv("../data/loic_prediction.csv", columns=["id", "str_hotel_cluster"], header=["id", "hotel_cluster"], index=False)

    log.info("~ • ~ END OF THE SCRIPT, DURATION: %s Hours ~ • ~"%(parseTime(time.time() - start_time)))
