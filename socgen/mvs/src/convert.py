# coding: utf-8
__author__ = 'LoicMDIVAD'

import os
import re
import time
from optparse import OptionParser

PARAM_OUTPUT = '~/mvs_convertion' + time.strftime("%Y_%m_%d_%H-%M-%S") + '.csv'
param_config = '/Users/LoicMDIVAD/Documents/workspace/python/fixed_fields_coverter/DUN_CONF.txt'
param_file = '/Users/LoicMDIVAD/Documents/workspace/python/fixed_fields_coverter/DUN2.txt'
param_output = '/Users/LoicMDIVAD/Documents/workspace/python/fixed_fields_coverter/DUN.csv'

class Mapper(object):
    def __init__(self, file_path, config_path):
        self.file_path = file_path
        self.config_path = config_path
        self.config = []

    #TODO: deal with not absolute path
    def _load_config(self):
        if os.path.isfile(self.config_path):
            with open(self.config_path, 'r') as conf:
                c = 0
                for line in conf:
                    key, val = tuple(line.split(';'))
                    try:
                        self.config.insert(c ,(key,int(re.search('\d+', val).group(0))))
                    except:
                        pass
                    c += 1
        else:
            raise Exception('Aucune configuration n\' a été retrouvée.')

    def _get_header(self):
        head = ''
        for key, val in self.config:
            head = head + key + ';'
        return head

    def _map_one(self, line):
        start = 0
        row = ''
        for col, limit in self.config:
            row = row + line[start:start+limit].strip() + ';'
        return row

    def _build_file(self):
        csv = open(param_output, 'w')
        csv.write(self._get_header() + '\n')

        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r') as data:
                for line in data:
                    csv.write(self._map_one(line) + '\n')

        csv.close()


    def process(self):
        self._load_config()
        self._build_file()

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="file", default='', help="Ficher mvs a trnasformer")
    parser.add_option("-c", "--config", dest="config", default='', help="Fichier de configuration")

    m = Mapper(param_file,param_config)
    m.process()




