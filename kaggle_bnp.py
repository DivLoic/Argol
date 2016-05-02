# coding: utf8
from __future__ import division
# imports
import sys
import time
import math
import random
import pandas as pd
import numpy  as np

#from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.cross_validation import train_test_split


SEED_STATE = 1
BAD = []

train = pd.read_csv('train.csv')
test = pd.read_csv('test.csv')

train = train.drop(BAD, axis=1)
test = test.drop(BAD, axis=1)

start_time = time.time()

# balance the traning dataset
#ones = train[train["target"] == 1]
#zeros = train[train["target"] == 0]

#df_droped, df_kept = train_test_split(ones, test_size=0.32, random_state=SEED_STATE) 
#train = pd.concat([zeros, df_kept])

# re shuffle the dataset
#np.random.seed(SEED_STATE)
#train = train.iloc[np.random.permutation(len(train))]

# features selection
all_features = train.drop(["ID", "target"], axis=1).columns
dict_types = all_features.to_series().groupby(train.dtypes).groups
dict_types = {k.name: v for k, v in dict_types.items()}
#int_columns = dict_types[u'int64']
#cat_columns = dict_types[u'object']
float_columns = dict_types[u'float64']

train = train[float_columns + ["ID", "target"]]
test = test[float_columns + ["ID"]]

# 25 features with the more missing values
#col_dead =  train.isnull().sum()
#col_dead.sort_values(inplace=True)

#train = train.drop(col_dead[-55:].index.tolist(), axis=1)
#test = test.drop(col_dead[-55:].index.tolist(), axis=1)

# data cleaning
for item in train.iteritems():
    train[item[0]] = train[item[0]].fillna(item[1].mean())

for item in test.iteritems():
    test[item[0]] = test[item[0]].fillna(item[1].mean())


#Boost = GradientBoostingClassifier(n_estimators=10,
#                                   max_depth=50, 
#                                   random_state=SEED_STATE)

bin_train = pd.read_csv("train_bin.csv")
bin_test  = pd.read_csv("test_bin.csv")

train = pd.merge(train, bin_train, on="ID")
test = pd.merge(test, bin_test, on="ID")

#print train[["ID", "v3.A", "v78"]].iloc[0:8,:]

#print("~~~~~~~ • END OF THE SCRIPT, DURATION: %ss •  ~~~~~~~"%(time.time() - start_time))
#sys.exit(0)


Boost = ExtraTreesClassifier(n_estimators=1200,
	max_features=30,
	criterion='entropy',
	min_samples_split=2,
	max_depth=30, 
	min_samples_leaf=2, 
	n_jobs = -1,
	random_state=SEED_STATE)

Boost.fit(train.drop(["ID", "target"], axis=1) , train["target"])

result = test[["ID"]]
result["PredictedProb"] = Boost.predict_proba(test.drop(["ID"], axis=1))[:,1].tolist()
result["PredictedBin"] = Boost.predict(test.drop(["ID"], axis=1))

binary_df = pd.read_csv("lmd_binary.csv")

result = pd.merge(result, binary_df, on="ID")

for idx, line in result.iterrows():
    if line["PredictedBin"] == line["binary"] and line["PredictedProb"] > 0.7:
        result.loc[idx, ("PredictedProb")] = (0.9999 + (line["PredictedProb"] / 10000))
    if line["PredictedBin"] == line["binary"] and line["PredictedProb"] < 0.3:
    	result.loc[idx, ("PredictedProb")] = line["PredictedProb"] / 10000

result.to_csv("lmd_submission_last.csv", columns=["ID","PredictedProb"], index=False)

print("~~~~~~~ • END OF THE SCRIPT, DURATION: %ss •  ~~~~~~~"%(time.time() - start_time))

