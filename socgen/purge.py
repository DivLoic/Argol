# coding: utf-8

"""
 Author: Loic DIVAD
 Goal: purger l'index autres en conservant les 8 dernières occurence de 
 chaques ids
 Please, see also: ...
"""

# imports
import os
import re
import json
import time
import logging
import urllib3
import pandas as pd
from datetime import datetime
from optparse import OptionParser

# configure logging
FORMAT = '%(asctime)s [ %(levelname)s ] : %(message)s'
logging.basicConfig(format=FORMAT)#, filename=os.environ["APP_LOG"] + "/purgeforce.log")
log = logging.getLogger()
log.setLevel(logging.INFO)

# Only mendatory environement varaibles
ENV = ["ES_HTTPS_ENDPOINT", "APP_USER", "APP_LOG", "APP_ES_PASSWD"]
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
				log.error("La variable d'environement %s est manquante"%e)
				print "La variable d'environement %s est manquante"%e
				sys.exit(1) 

def parseTime(sec):
	m, s = divmod(sec, 60)
	h, m = divmod(m, 60)
	return "%d:%02d:%02d" % (h, m, s)

def es_json_parse(datum):
	"""
	take a string input and return dict like:
	{'hits': {'hits': {'_source': [] }}} etc ...
	param
	----------
		datum :string response from elasticsearch
	return
	----------
		:dict from string response from elasticsearch
	"""
	try:
		hits = datum["hits"]["hits"]
	except KeyError:
		log.error("Aucun hits n'a été retourné par l'index :%s"%esindex)
		sys.exit(1)
	return hits

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
		:dict from urllib3.util.make with user/password
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

def build_query(_id, source, default_size=5000):
	"""
		build_query(_id="1202", source="mysource", default_size=50000) build
		a es doc for a bool query and return it as a string
	params
	----------
		_id :string from the elasticsearch key Identifiant 
		source :string from the elasticsearch key fichierSource
	return
	----------
		:string body of the elasticsearch research for the following query
		get all docs with key Identifiant: _id AND fichierSource: source
	"""
	doc = { "size": default_size, "query": { "bool": { "must": [] } } }
	doc["query"]["bool"]["must"].append({"match": {"Identifiant": _id}})
	doc["query"]["bool"]["must"].append({"match": {"fichierSource": source}})

	return json.dumps(doc)

def build_delete_ids(all_ids):
	"""
	param
	----------
		all_ids :dict of string _id from es like: 
		["Ud9FNgm0Qt6HAXD3sv6vjg", "-02IGaCjRQyR4q_3BfDzCA"]
	return
	----------
		:string body of the elasticsearch query used to delete all the 
	"""
	doc = {"query": {"constant_score": {"filter": {"terms": {"_id": [] }}}}}
	doc["query"]["constant_score"]["filter"]["terms"]["_id"] = all_ids

	return json.dumps(doc)

def es_delete(eshost, esindex, estype, doc_body=""):
	"""
	param
	----------
		eshost :string, es http endpoint
		esindex :string, name of the index
		estype :string, type of es doc (PP/PM)

	return
	----------
		: urllib3.HTTPResponse from elasticserach
	"""
	eslastisearch_queries = "%s/%s/%s/_query"%(eshost,esindex,estype)
	res = HTTP.request("DELETE", eslastisearch_queries, body=doc_body, headers=es_header())

def es_docs(eshost, esindex, estype, essize=50000, skip=0):
	"""
	es_docs(eshost=http..., esindex=dkycibdr, estype=P(P/M), essize=50000, skip=0)
	params
	----------
		eshost :string es http endpoint
		esindex :string name of the index
		estype :string type of es doc (PP/PM)

	return
	----------
		:list of python dist from ES key ["hist"]["hits"]
	""" 
	hits = list()
	eslastisearch_queries = "%s/%s/%s/_search?size=%s"%(eshost,esindex,estype,essize)
	res = HTTP.request("GET", eslastisearch_queries, body="", headers=es_header())
	datum = json.loads(res.data)
	hits = es_json_parse(datum)

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
	hits = es_json_parse(datum)
	datum = sorted(hits, key= lambda y: y["_source"]["timestamp"])
	return map(lambda y: y["_id"], datum)[:-leave]
	
def uniqueIndex(documents, threshold,  fields=["Identifiant", "fichierSource", "timestamp"]):
	"""
	uniqueIndex(documents=, fields=["Prenom", "Nom", "Identifiant"])
	return en Empty DataFrame if document is an empty list
	retun en list of unique tuple else
	params
	----------
		documents :list of python dict from ES
		threshold :int under witch we treat the element
		fields :list of string keys 

	return
	----------
		:list of unique tuple ('Identifiant', 'fichierSource')
	"""
	if len(documents) == 0: list()
	df = pd.DataFrame(documents)[fields]
	df = df.groupby(["Identifiant", "fichierSource"]).count()
	df = df.rename(columns={"timestamp": "count"})
	return df.index.tolist()

if __name__ == "__main__":

	# lookup env
	testEnv()

	# lookup args
	optparser = OptionParser()
	optparser.add_option("-i", "--esindex", dest="esindex", default="FORCE", help="")
	optparser.add_option("-t", "--estype", dest="estype", default="PP", help="elasticsearch type of doc (PP or PM)")
	optparser.add_option("-l", "--docleft", dest="left", default=8, type="int", help="number of documents to leave in the index")
	optparser.add_option("-s", "--docstep", dest="step", default=2000, type="int", help="number of documents for each req")

	options, args = optparser.parse_args()

	esuser = os.environ["APP_USER"]
	eshost = os.environ["ES_HTTPS_ENDPOINT"]
	esidx  = os.environ[options.esindex].lower()

	people = list()
	unique = list()

	log.info("Comptage du nombre de documents de type %s dans l'index: [%s]"%(options.estype, options.esindex))
	stop_size = es_count_docs(eshost, esidx, options.estype) # combien il y a t il doc 
	stop_size = min(stop_size, 30000000)
	skip_size, step_size = (0 , options.step)

	start_time = time.time()



	for x in range(0, stop_size+step_size, step_size):
		log.info("Reccupération des documents entre [%s-%s]"%(skip_size, skip_size+step_size))
		people = es_docs(eshost=eshost, esindex=esidx, estype=options.estype, essize=step_size, skip=skip_size)
		unique = unique + uniqueIndex(people, threshold=options.left)
		unique = pd.unique(unique).tolist() 

		skip_size = skip_size + step_size # decalage sur l'index

	log.info("Fin de la création de la liste de tiers! %s heures"%(parseTime(time.time() - start_time)))

	for doc in unique:
		es_consumers_json = build_query(doc[0], doc[1]) # construction du json & recupération de la list d' _id
		doc_ids = es_shift_id(doc=es_consumers_json, eshost=eshost, esindex=esidx, estype=options.estype, leave=options.left)
		if len(doc_ids) > 0:
			log.info("Supression des documents pour le tuple : (%s, %s)."%(doc[0], doc[1]))
			es_delete(eshost=eshost, esindex=esidx, estype=options.estype, doc_body=build_delete_ids(doc_ids)) #supression
		
	log.info("Fin de purge avec succes! %s heures"%(parseTime(time.time() - start_time)))	
