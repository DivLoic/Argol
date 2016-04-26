# coding: utf-8

"""
 Author: Loic DIVAD
 Goal: ...
 Please, see also: ...
"""
# imports
import os
import re
import json
import logging
import urllib3
import pandas as pd
from datetime import datetime
from optparse import OptionParser


# configure logging
FORMAT = '%(asctime)s [ %(levelname)s ] : %(message)s'
logging.basicConfig(format=FORMAT)
log = logging.getLogger()
log.setLevel(logging.DEBUG)

# Only mendatory environement varaibles
ENV = ["ES_HTTPS_ENDPOINT", "APP_USER", "APP_ES_PASSWD"]
HTTP = urllib3.PoolManager()# http client

def testEnv():
	"""
	for all name in ENV list is the name is an environement variable?
	if not stop the script because we need it
	params
	----------
	return
	----------
		None 
	"""
	_env = os.environ.keys()
	for e in ENV:
			if e not in _env:
				print "La variable d'environement %s est manquante"%e
				sys.exit(1) 

def es_passwd():
	"""
	es_passwd() return a password from the file: 
	scripts/config/password/es_passwd.txt
	params
	----------
	return
	----------
		:string password from a file
	"""
	password_file = open(os.environ["APP_ES_PASSWD"]+"/es_passwd.txt")
	password = re.findall("passwd=(.*)",password_file.readline())[0]
	password_file.close()

	return password

def es_header():
	"""
	es_header() write a http header with the correct user:password
	for elasticsearch. pass this as headers for urllib3.PoolManager.request.
	params
	----------
	return
	----------
	"""
	return urllib3.util.make_headers(basic_auth="superuser:%s"%(es_passwd()))

def es_count_docs(eshost, esindex, estype):
	"""
	params
	----------
		eshost :string, es http endpoint
		esindex :string, name of the index
		estype :string, type of es doc (PP/PM)

	return
	----------
		 :int number of documents in the index		
	"""

	eslastisearch_queries = "%s/%s/%s/_count"%(eshost,esindex,estype)
	res = HTTP.request("GET", eslastisearch_queries, body="", headers=es_header())

	return json.loads(res.data)[u'count']

def build_query(_id, source, default_size=50000):
	"""
	"""
	doc = { 
	 "size": default_size,
	 "query": { "bool": { "must": []}}
	}

	doc["query"]["bool"]["must"].append({"match": {"Identifiant": _id}})
	doc["query"]["bool"]["must"].append({"match": {"fichierSource": source}})

	return json.dumps(doc)


def es_docs(eshost, esindex, estype, essize=50000, skip=0):
	"""
	es_docs(eshost=http..., esindex=dkycibdr, estype=PP(M), essize=50000, skip=0)
	params
	----------
		eshost :string: es http endpoint
		esindex :string: name of the index
		estype :string: type of es doc (PP/PM)

	return
	----------
		list of python dist from ES key ["hist"]["hits"]
	""" 
	hits = list()
	eslastisearch_queries = "%s/%s/%s/_search?size=%s"%(eshost,esindex,estype,essize)
	res = HTTP.request("GET", eslastisearch_queries, body="", headers=es_header())
	datum = json.loads(res.data)
	try:
		hits = datum["hits"]["hits"]
	except KeyError:
		print "[ERROR]: Erreur lors du requetage de l'index: %s"%esindex

	return map(lambda y : y["_source"], hits)

def es_shift_id(doc, eshost, esindex, estype, essize=50000, leave=8):
	"""
	es_shift_id(doc='{...}', leave=8) ask all docs to elasticsearch with
	the id and source, return all es keys '_id' (except for 5, 6, 8, 'leave' last). 

	params
	----------
		doc :Dict {"Identifiant": ..., "fichierSource": ...}
		leave :int number of ids we choose to not delete 
	return
	----------
		:List of string corresponding to es '_id' key.
	"""
	hits = list()
	eslastisearch_queries = "%s/%s/%s/_search"%(eshost,esindex,estype)
	res = HTTP.request("GET", eslastisearch_queries, body=doc, headers=es_header())
	datum = json.loads(res.data)
	datum = sorted(datum, key= lambda y: y["_source"]["timestamp"])
	return map(lambda y: y["_id"], datum["hits"]["hits"])[:-leave]
	
	

def uniqueDf(documents, fields=["Identifiant", "fichierSource", "timestamp"]):
	"""
	uniqueDf(documents=, fields=["Prenom", "Nom", "Identifiant"]) 
	params
	----------
		documents :List of python dict from ES

	return
	----------
		pandas.DataFrame with unique couple of firstname, lastname
	"""
	#def valid(y):
	#	for f in fields:
	#		if f not in y.keys():
	#			return False
	#	return True

	#documents = filter(valid, documents)

	df = pd.DataFrame(documents)[fields]
	df = df.groupby(["Identifiant", "fichierSource"]).first()
	#df = df.groupby(["Identifiant", "fichierSource"], axis=0).first()#.last()
	return df



if __name__ == "__main__":


	# lookup env
	testEnv()

	# lookup args
	optparser = OptionParser()
	optparser.add_option("-i", "--esindex", dest="esindex", default="FORCE", help="")
	optparser.add_option("-t", "--estype", dest="estype", default="PP", help="")
	
	options, args = optparser.parse_args()

	# uniqueDf([{"a": "1","b": "10"},{"a":"2","b": "20"},{"a": "3","b": "30"},{"a": "4","b": "40"}], fields=["a", "b"])

	esuser = os.environ["APP_USER"]
	eshost = os.environ["ES_HTTPS_ENDPOINT"]
	esidx  = os.environ[options.esindex].lower()

	log.info("Reccupération des documents entre [0-50000]")
	people = es_docs(eshost=eshost, esindex=esidx, estype=options.estype, essize=50000, skip=0)
		
	# essize = es_count_docs(eshost, esidx, options.estype)
	# print es_count_docs(eshost, esidx, "PP")
	# print es_count_docs(eshost, esidx, "PM")
	#
	#
	#

	#print len(people)
	log.info("Aggrégation de champs discriminants")
	ids = uniqueDf(people).index.tolist()
	for unique_id in ids:
		log.info("Supression des documents pour le tuple : (%s, %s)."%unique_id)
		es_consumer = build_query(unique_id[0], unique_id[1])
		print es_consumer
		print es_shift_id(doc=es_consumer, eshost=eshost, esindex=esidx, estype=options.estype, leave=2)#, leave=2)
		break 
