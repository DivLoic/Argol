# -*- coding: utf-8 -*-
__author__ = 'LoicMDIVAD'

import unittest
from es_trace_conf import *
from es_trace_parcer import *


class MyTestCase(unittest.TestCase):

    def setUp(self):
        self.parcer = Parser('es_trace_exemple.json')

    def test_read_target(self):
        """
        Vérifie la bonne lecture du fichier Json d'exemple et confire que les documents
        ont bien été insérés dans un type de donnée list
        :return:
        """
        assert type(self.parcer.hits) is list

    def test_mendatory_key(self):
        """
        La configuration spécifie une clef obligatoire. "information" (sur kyc 2015)
        les document qui n'on pas cette clef sont rejetés du parser.
        On évite KeyError Exception
        :return:
        """
        error_doc = filter(lambda y: "info" not in y.keys(), self.parcer.hits)
        self.assertEquals(0,len(error_doc))

    def test_dump_sting(self):
        """
        Test la convertion string -> dict.
        Rappel: elasticsearch introduit des \" & \n génant dans la version préscédante
        :return:
        """
        Parser.dump_string("{\"hello\": \"world\"}")
        Parser.dump_string('{"hello": "world"}')



if __name__ == '__main__':
    unittest.main()
