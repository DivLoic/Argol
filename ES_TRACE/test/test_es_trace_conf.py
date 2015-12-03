#-*- coding: utf-8 -*-
__author__ = 'LoicMDIVAD'

import re
import yaml
import unittest
from es_trace_conf import *

FILE = os.path.dirname(os.path.abspath(__file__)) + '/../'
HERE = os.path.abspath(FILE) + '/'


class EsTraceTestCase(unittest.TestCase):

    def setUp(self):
        # test_checking_env
        os.environ['KYC_ES_TRACE_PARCER_LOG'] = os.getcwd()
        self.conf = Config('test_es_trace_conf.yaml')

    def test_conf_loader(self):
        """
        Vérifie que l'object configuration crée dispose bien du bon fichier.
        :return:
        """
        assert type(self.conf.default_conf) is dict


    def test_rewrite(self):
        """
        La configuration (es_trace_parcer.py) doit être lu puis écrite (.HARD_CONF.yaml)
        en prenant compte des variables d'environnement. Le dernier ficher est chargé sous forme de dict.
        :return:
        """
        Config.rewriteConfig(HERE + "es_trace_conf.yaml", ".HARD_CONF.yaml")
        with open(FILE_CONFIG, 'r') as f:
            conf = yaml.load(f)

        self.assertTrue(type(conf) is dict)


if __name__ == '__main__':
    unittest.main()
