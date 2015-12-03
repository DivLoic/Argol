# -*- coding: utf-8 -*-
__author__ = 'LoicMDIVAD'

import os
import json
import pandas as pd
from optparse import OptionParser
from  es_trace_conf import Config

class Parser(object):
    """

    """

    def __init__(self, target, config_file='es_tarce_conf.yaml'):
        self.conf = Config(config_file)
        self.targetfile = target
        self.target = os.path.abspath(target)
        self.hits = []

        try:
            with open(self.target, 'r') as tg:
                line = tg.read()
                self.hits = json.loads(line)['hits']['hits']
        except IOError as io:
            self.hits = list()
            print io
        except:
            # TODO: Préparé un arret générale du scirpt
            self.hits = list()
            pass

        self.hits = map(lambda y: y['_source'], self.hits)
        # TODO: remettre en question le filtrage des documents éronés
        # self.hits = filter(lambda y: set(self.conf.get['key']).issubset(set(y.keys())), self.hits)

    @staticmethod
    def dump_string(str):
        return json.loads(str)

    @staticmethod
    def diginto(array):
        """

        :param dict:
        :return:
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

    def writeCSV(self, content):
        pass

    def matchPattern(self, KeyValues, tpl=False):

        for filename in self.conf.get["files"]:
            matches = filter(lambda y: y.keys()[0] == self.conf.get["files"][filename]["pattern"], KeyValues)
            # TODO: Trancher sur l'unicité du pattern
            if len(matches) == 1:
                return filename
        return 'Not_found'

    def restrictToPattern(self, pattern, keyValues):
        # TODO: Tranher sur l'unicité des clefs/champs
        matterKeys = self.conf.get['files'][pattern]["keys"]
        return filter(lambda y: y.keys()[0] in matterKeys, keyValues)


    def process(self):
        """

        :return:
        """
        dicts_containers = dict()
        for files in self.conf.get['files'].keys():
            dicts_containers[files] = []

        for hit in self.hits:
            dicts_hit = Parser.dump_string(hit[self.conf.mainKey])
            cells_hit = Parser.diginto(dicts_hit)
            pattern = self.matchPattern(cells_hit)

            cells_hit = self.restrictToPattern(pattern, cells_hit)
            # turn [{'one': 1}, {'two': 2}] in {'one': 1, 'two': 2}
            row = {k: v for cell in cells_hit for k, v in cell.items()}
            dicts_containers[files].append(row)

        for d in dicts_containers.keys():
            print "THIS IS THE DICT %s ---------> %s"%(d, len(dicts_containers[d]))
            pd.DataFrame(dicts_containers[d]).to_csv('test.csv', sep=";")



if __name__ == '__main__':
    print " -- Start as main file"

    optparser = OptionParser()
    optparser.add_option("-t", "--target",
                         dest="target", default="es_trace_exemple.json", help="Fichier Json a convertire")
    optparser.add_option("-e", "--extract",
                         dest="extract", default="FALSE", help="Extraction ou non de la tarce depuis ES")

    options, args = optparser.parse_args()

    parser = Parser(options.target)
