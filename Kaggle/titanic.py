# coding: utf-8

"""
	author: Loïc M. DIVAD
	date: 2016-04-25
	see also: https://www.kaggle.com/c/titanic
"""

# imports
import sys
import time
import math
import random
import logging
import pandas as pd
import numpy  as np

from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import train_test_split

# constantes
SEED_STATE = 1
KFOLD_NUMB = 10
TARGET = "Survived"
NON_FEATURES = ["Pclass", "Cabin", "Name", "Ticket", "Embarked", "Cabin.U"]
LOG_FORMAT = '%(asctime)s [ %(levelname)s ] : %(message)s'

# logging
import warnings
warnings.filterwarnings("ignore")

logging.basicConfig(format=LOG_FORMAT)
log = logging.getLogger()
log.setLevel(logging.INFO)

def parseTime(sec):
	m, s = divmod(sec, 60)
	h, m = divmod(m, 60)
	return "%d:%02d:%02d" % (h, m, s)

def binary(df, column, values=["1", "0"]):
	"""
	params
	----------

	return
	----------
	"""
	for value in values:
		df["%s.%s"%(column,value)] = map(lambda y: 1 if y == value else 0, df[column])
	return df

def dataCleaning(df, inPlace=True, age_na=0, fare_na=0, toDrop=NON_FEATURES):
	"""
	params
	----------
	return
	----------
	"""
	
	#df["Age"].fillna(age_na, inplace=inPlace)
	df["Fare"].fillna(fare_na, inplace=inPlace)

	df["Cabin"].fillna("unknow", inplace=inPlace)
	df["Cabin"] = map(lambda y: y[0].upper(), df["Cabin"])

	df["Embarked"].fillna("C", inplace=inPlace)

	df["Sex"] = df["Sex"].map({'female': 0, 'male': 1}).astype(int)

	bridges = sorted(df["Cabin"].unique())

	df = binary(df, column="Embarked", values=['C', 'Q', 'S'])
	df = binary(df, column="Pclass", values=['1', '2', '3'])
	df = binary(df, column="Cabin", values=bridges)

	return df.drop(toDrop, axis=1)

def regressionTreeBuilder(dftrain, dftest):
	"""
	regressionTreeBuilder() return a instance of sklearn.tree.DecisionTreeRegressor
	able to predict the age of  
	params
	----------
	return
	----------
	"""
	train = dftrain.dropna(inplace=False)
	test = dftest.dropna(inplace=False)
	titanic = pd.concat([train, test]).drop(["Survived"], axis=1)
	titanic = titanic.iloc[np.random.permutation(len(titanic))]
	reg_tree = DecisionTreeRegressor(min_samples_split=11)
	reg_tree.fit(titanic.drop(["PassengerId", "Age"], axis=1),titanic["Age"])
	return reg_tree


if __name__ == "__main__":
	start_time = time.time()

	### -- TRAIN
	log.info("Loading dataset: data/train.csv")
	train = pd.read_csv("data/train.csv")


	median_age  = train["Age"].median()
	fare_median = train["Fare"].median()

	train = dataCleaning(train, age_na=median_age)
	train = train.drop("Cabin.T", axis=1)

	### --- TEST
	log.info("Loading dataset: data/test.csv")
	test = pd.read_csv("data/test.csv")

	test = dataCleaning(test, age_na=median_age, fare_na=fare_median)

	age_forest = regressionTreeBuilder(train, test)

	for idx, row in train.iterrows():
		label = row["Age"]
		features = row.drop(["PassengerId", "Survived", "Age"]).tolist() 
		if(math.isnan(label)):
			preticted = age_forest.predict(features)
			#print "%s --  %s"%(label,age_forest.predict(features))
			train["Age"][idx] = math.floor(preticted)

	for idx, row in test.iterrows():
		label = row["Age"]
		features = row.drop(["PassengerId", "Age"]).tolist() 
		if(math.isnan(label)):
			preticted = age_forest.predict(features)
			#print "%s --  %s"%(label,age_forest.predict(features))
			test["Age"][idx] = math.floor(preticted)

	### --- FIT

	forest = RandomForestClassifier(
		n_estimators=200,
		max_depth=8,
		min_samples_split=12,
		#min_samples_leaf=1,
		random_state=SEED_STATE,
		n_jobs=-1
	)

	log.info("Start building the tree -> ")
	forest.fit(train.drop(["PassengerId", TARGET], axis=1), train[TARGET])
	log.info("Stop building the tree  ->  ")

	log.info("Start predicting: ")
	test[TARGET] = forest.predict(test.drop(["PassengerId"], axis=1)).astype(int)

	log.info("Start pwrinting the result: ")
	test.to_csv("data/loic_prediction.csv", columns=["PassengerId", TARGET], index=False)

	log.info("~ • ~ END OF THE SCRIPT, DURATION: %s Hours ~ • ~"%(parseTime(time.time() - start_time)))
