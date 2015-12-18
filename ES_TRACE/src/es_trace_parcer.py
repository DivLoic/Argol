# -*- coding: utf-8 -*-
__author__ = 'LoicMDIVAD'

import os
import sys
import json
import pandas as pd
from datetime import datetime
from optparse import OptionParser
from es_trace_conf import Config
from es_trace_logger import TrcLogger

class Parser(object):
    """

    """

    def __init__(self, target, config_file='es_trace_conf.yaml'):
        self.conf = Config(config_file)
        self.targetfile = target
        self.target = os.path.abspath(target)
        self.log = TrcLogger(self.conf.get["es_trace_log"])
        self.hits = []

        try:
            with open(self.target, 'r') as tg:
                line = tg.read()
                self.hits = json.loads(line)['hits']['hits']
        except IOError as io:
            self.hits = list()
            self.log.get.error('Le fichier  - %s - est introuvable.'%self.targetfile)
            self.log.get.error('Aucun fichier à l\'adresse : %s '%self.target)
            sys.exit(1)
        except Exception, e:
            # TODO: Préparé un arret générale du scirpt
            self.hits = list()
            print e
            pass

        len_init  = len(self.hits)
        self.hits = map(lambda y: y['_source'], self.hits)
        self.hits = filter(lambda y: self.conf.mainKey in y.keys(), self.hits)
        self.log.get.info('Nombre de documents chargés dans le parseur : %s'%len(self.hits))
        self.log.get.warn('La clef pricipale { '+self.conf.mainKey+' } est absente de %s document(s)'%(len_init - len(self.hits)))
        # TODO: remettre en question le filtrage des documents éronés
        #self.hits = filter(lambda y: set(self.conf.get['key']).issubset(set(y.keys())), self.hits)


    @staticmethod
    def dump_string(str):
        return json.loads(str)

    @staticmethod
    def diginto(array):
        """
        Prend en paramètre un dict et retorune une python list de clef valeurs.
        [ {'a':'1'} , {'b':'2'} , {'c':'3'} , {'d':'4'} ]  
        :param dict:
        :return valus <list>:
        """
        values = []
        for key in array.keys():
            if type(array[key]) is list:
                for a in array[key]:
                    values += Parser.diginto({key: a})
                pass
            elif type(array[key]) is dict:
                values += Parser.diginto(array[key])
            else:
                values.append({key: array[key]})

        return values

    @property
    def _now(self):
        return datetime.now().strftime("%Y-%m-%d")    

    def matchPattern(self, KeyValues, tpl=False):

        for filename in self.conf.get["files"]:
            copy = list(KeyValues)
            matches = filter(lambda y: y.keys()[0] == self.conf.get["files"][filename]["pattern"], copy)
            # TODO: Trancher sur l'unicité du pattern
            if len(matches) > 0:
                return filename
        
        #print filename
        #print self.conf.get["files"][filename]["pattern"]
        #print len(matches)
        return None

    def head(self, filename, df):
        headMap = dict()
        for idx, k in enumerate(self.conf.get["files"][filename]["keys"]):
            headMap[k] = self.conf.get["files"][filename]["fields"][idx]
            df.rename(columns=headMap, inplace=True)
        return headMap    

    def restrictToPattern(self, pattern, keyValues):
        # TODO: Trancher sur l'unicité des clefs/champs
        if pattern != None:
            matterKeys = self.conf.get['files'][pattern]["keys"]
            return filter(lambda y: y.keys()[0] in matterKeys, keyValues)
        else:
            return list()

    @staticmethod
    def extract():
        print "I AM ASKING ES"

    def process(self):
        """

        :return:
        """
        dicts_containers = dict()
        for files in self.conf.get['files'].keys():
            dicts_containers[files] = []

        for hit in self.hits:
            try:
                dicts_hit = Parser.dump_string(hit[self.conf.mainKey])
            except ValueError as ve:
                self.log.get.debug("La clef (( " + self.conf.mainKey +" )) ne présente pas un format json.")
                self.log.get.debug(str(ve))
                dicts_hit = {"error": "ValueError"}
            
            cells_hit = Parser.diginto(dicts_hit)
            cells_hit += Parser.diginto(hit)
            pattern = self.matchPattern(cells_hit)

            # trun [{'a':'1'}, {'b':'2'}, {'c':'3'}] into [{'a':'1'}]
            cells_hit = self.restrictToPattern(pattern, cells_hit)

            # turn [{'one': 1}, {'two': 2}] in {'one': 1, 'two': 2}
            row = {k: v for cell in cells_hit for k, v in cell.items()}
            if pattern is not None: dicts_containers[pattern].append(row)

        for d in dicts_containers.keys():

            path = self.conf.get['es_trace_delibery'] + '/'
            out_file =  d + '_' + self.conf.get['es_trace_out']
            
            df = pd.DataFrame(dicts_containers[d])
            self.head(d, df)
            df.to_csv(path + out_file + self._now + '.csv', sep=";", encoding = 'latin-1')

            self.log.get.info("Ecriture de ( %s ) lignes pour le fichier %s"%(len(dicts_containers[d]),d))



if __name__ == '__main__':

    optparser = OptionParser()
    optparser.add_option("-t", "--target",
                         dest="target", default="test/test_es_trace.json", help="Fichier Json a convertire")
    optparser.add_option("-e", "--extract",
                         dest="extract", default=False, help="Extraction ou non de la tarce depuis ES")

    options, args = optparser.parse_args()  
    
    if options.extract:
        options.target = 'es_trace_extract.json'
        Parser.extract()

    parser = Parser(options.target)

    parser.log.get.info('Parsing du fichier : %s'% parser.targetfile)

    parser.process()

    parser.log.delimite()


