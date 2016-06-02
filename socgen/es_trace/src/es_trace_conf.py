# -*- coding: utf-8 -*-
__author__ = 'LoicMDIVAD'

import os
import re
import yaml
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__)) + '/../'
FILE_CONFIG = '.HARD_CONF.yaml'

class Config(object):
    """
        Objetc configuration du parseur.
        Lit le fichier de configuration obligatoirement présent à la racine du dossier ES_TARCE/
        (es_trace_conf.yaml). Comprend les varibales d'environnement unix tel que ${Varibale}

    """

    def __init__(self, csv_file='es_trace_conf.yaml'):
        self.nonEnv = list()
        self.rewriteConfig(HERE + csv_file, FILE_CONFIG)       
        with open(HERE + FILE_CONFIG, 'r') as f:
            self.default_conf = yaml.load(f)

    @property
    def get(self):
        return self.default_conf

    @property
    def mainKey(self):
        return self.default_conf["primary"]

    @property
    def undefined(self):
        return (len(self.nonEnv), list(self.nonEnv))


    def rewriteConfig(self, input, output):
        """
        fonction io, lit le fichier es_trace_conf.yaml et remplace les varibales d'environnemnt dans
        .HARD_CONF.yaml le fichier. Ce fichier est lu et chargé dans self.default_conf
        :param input: nom du ficher à parser
        :param output: fichier ou les variables sont écrites
        :return: void
        """
        lines_in = []
        with open(input, 'r') as input_file:
            for line in input_file:
                lines_in.append(line)

        for l, line in enumerate(lines_in):
            variables = re.findall("\${(.*?)\}", line)
            for var in variables:
                exist, value = Config.isEnv(var)
                if not exist: self.nonEnv.append(var)
                lines_in[l] = lines_in[l].replace('${'+var+'}', value)


        with open(HERE + output, 'w') as output_file:
            for line in lines_in:
                output_file.write(line)


    @staticmethod
    def isEnv(variable):
        """
        Prend un nom de variable d'environement et renvoie  un tuple
        (exitance, valeur) de type (Boolean, String)
        Rencvie [String à déterminer] si la valeur n'est pas définie.
        :param variable <String>:
        :return tuple <Boolean, String>: 
        """
        try:
            return (True , os.environ[variable])
        except Exception as e:
            #TODO: Trover une meilleur indication pour l'absence de variable
            return (False, '#<variable = '+variable+' absente>')



if __name__ == '__main__':
    print 'Vérification de la configuration. '