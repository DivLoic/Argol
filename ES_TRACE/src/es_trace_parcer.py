# -*- coding: utf-8 -*-
__author__ = 'LoicMDIVAD'

import os
import json
from optparse import OptionParser
from  es_trace_conf import Config

class field(object):
    pass


class Parser(object):
    """

    """

    def __init__(self, target):
        self.conf = Config()
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

        self.hits = map(lambda y: y["_source"], self.hits)
        self.hits = filter(lambda y: self.conf.get["key"] not in y.keys(), self.hits)

    @staticmethod
    def dump_string(str):
        return json.loads(str)

if __name__ == '__main__':
    print " -- Start as main file"

    optparser = OptionParser()
    optparser.add_option("-t", "--target",
                         dest="target", default="es_trace_exemple.json", help="Fichier Json a convertire")
    optparser.add_option("-e", "--extract",
                         dest="extract", default="FALSE", help="Extraction ou non de la tarce depuis ES")

    options, args = optparser.parse_args()

    parser = Parser(options.target)
