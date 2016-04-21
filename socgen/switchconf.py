# coding: utf-8
__author__ = 'LoicMDIVAD'

"""
 Author: Loic DIVAD
 Goal: "bla bla bla"
 Please, see also: ...
"""

import os
import sys
import logging
import pandas as pd
from optparse import OptionParser

# CONSTANTES 
ENV = ["FIC_ECHEC"]
SKIP_CONF_LINES = 5
PREFIX_CONF = "APP_CONFIG"
HERE = os.environ['APP_EXE'] + '/fixfields/'


# configure logging
FORMAT = '%(asctime)s [ %(levelname)s ] : %(message)s'
logging.basicConfig(format=FORMAT)
log = logging.getLogger()
log.setLevel(logging.DEBUG)

def testEnv():
	"""
	testEnv() for all name in ENV list is the name is an environement variable?
	/!\ kill the script if a variable is missing, everythings in ENV is mandatory.
	params
	---------
	
	return
	---------
		None
	"""
	_env = os.environ.keys()
	for e in ENV:
			if e not in _env:
				log.error("La variable d'environement %s est manquante"%e)
				sys.exit(1) 

def configFormat(conf_name, header_lignes_number=SKIP_CONF_LINES):
	"""
	configFormat(conf_name='...', header_lignes_number=5) find and read a configuration file.
	/!\ kill the script if the file is not found
	return 3 elements, the source, the format and the meta data 

	params
	----------
		conf_name :string name of a source like AL99LCSC
		header_lignes_number :int number of lines of meta data
	return
	----------
		source :string BDR, GRC, RCT OR FORCE
		lines :int format of the configuration (8, 14, 18, 21 lines, etc ...)
		header_lignes :List the n first ligne of meta data as string
 	"""
	lines = 0
	header_lignes = []
	file_found = False
	for root, subdirs, files in os.walk(os.environ[PREFIX_CONF]):
		clean_files = filter(lambda y: "OLD" not in y, files)
		if conf_name in clean_files:
			file_found = True
			break

	if not file_found:
		log.error("Le fichier %s n'est pas présent dans: %s"%(conf_name, os.environ[PREFIX_CONF]))
		sys.exit(1)

	source = root.split('/')[-1].upper()
	path = root + '/' + conf_name

	with open(path) as conf:
		for line in conf:
			lines = lines + 1 
			if lines <= header_lignes_number:
				header_lignes.append(line)

	lines = lines - header_lignes_number
	return (source, lines, header_lignes)

def legalEntity(filename):
	"""
    legalEntity(filename="...") does the file name containes PP or PM ?

	params
	----------
		filename :string configuration file without extention (like AL99LCSC)
	return
	----------
		:string PP or PM depending of the nature of the personne
	"""
	# should ever end with a P or a C
	return u'PP' if filename[-1] == "P" else u'PM'


def df4format(entity, size):
	"""
	df4format(entity=, size=18)
	params
	----------
		entity :string
		size :int 

	return 
	----------
		pandas.DataFrame indexed by the column index

	"""
	filename = "ref_force_%s_%s.csv"%(entity, size)
	try:
		ref_format_df = pd.read_csv(HERE + filename, sep=";")
	except IOError:
		log.error("IOError? le fichier de référence est introuvable.")
		log.error("Aucun format %s n'est prévu pour l'entité legal: %s."%(size, entity))
		sys.exit(1)

	ref_format_df = ref_format_df.set_index(["index"])

	return ref_format_df

def writeConf(filename, root, meta_data, df):
	"""
	params
	----------
		filename :string AL99LCSC.conf like
		meta_data :List of string, 5 first line of the old file 
		df :pandas.DataFrame took from the reference files
	return
	----------
		None

	"""

	with open(root +"/"+ filename, "w") as f:
		for meta in meta_data:
			f.write(meta)

		for idx, item in df.iterrows():
			f.write("%s;%s\n"%(item["label"],item["info"]))



if __name__ == "__main__":

	# lookup env
	testEnv()

	# lookup args
	optparser = OptionParser()
	optparser.add_option("-f", "--file", dest="file", default="", help="nom du fichier source")
	optparser.add_option("-s", "--size", dest="size", default="", help="nombre de champs du csv (8, 14, 18, 21)")

	options, args = optparser.parse_args()
	file_conf_name = "%s.conf"%options.file

	log.info("Nom du fichier = %s, pour une taille = %s"%(options.file, options.size))

	actual_source, actual_format, actual_meta = configFormat(conf_name=file_conf_name)

	log.info("La source = %s, correspond à une configuration = %s"%(options.file, actual_source))

	# check diff
	if str(actual_format) == options.size:
		print actual_format
		print options.size
		log.warning("La configuration actuelle présente bien %s champs"%options.size)
		log.warning("Sortie du programme avec le code erreur: 1")
		sys.exit(1)

	log.info("La configuration actuelle présente %s champs"%actual_format)

	#  
	ref_df = pd.read_csv(HERE + "ref_force.csv", sep=";")
	ref_df = ref_df.set_index(["filename"])

	ref_format_df =  df4format(entity=actual_meta[1][:2].lower(), size=options.size)

	root = os.environ["KYC_CONF_%s"%actual_source]
	old_file = root+'/'+file_conf_name
	new_file = root+'/OLD_%s_'%actual_format+file_conf_name
	
	os.rename(old_file, new_file)
	log.info("Changement du nom de l'ancien fichier en : %s. "%(new_file.split(u'/')[-1]))

	writeConf(file_conf_name, root, actual_meta, ref_format_df)
	log.info("Fin de l'écriture du nouveau fichier %s ."%file_conf_name)













