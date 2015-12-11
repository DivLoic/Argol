# -*- coding: utf-8 -*-
__author__ = 'LoicMDIVAD'

import logging
import unittest
from src.es_trace_conf import *
from src.es_trace_parcer import *

FILE = os.path.dirname(os.path.abspath(__file__))
HERE = os.path.abspath(FILE) + '/'

class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.parcer = Parser(HERE + 'test_es_trace.json', 'test/test_es_trace_conf.yaml')
        self.config = self.parcer.conf.get
        self.parcer.log.log4test()
        self.hit = {"info": "{\"useless\": \"two\", \"query\": {\"emptytabs\": [1,2,3, {\"type\": \"dict\", "
                        "\"place\": \"in a liste\", \"level\":{\"lowerlevle\": \"ever\"}}], \"hash\": \"\", "
                        "\"order\": false, \"mapping\": false}, \"timestamp\": \"2015-01-03T00:00:00\", "
                        "\"page\": \"PageY\", \"type\": \"Fire,Water,Plante,Fight\", \"pages\": \"\", "
                        "\"id\": \"3520e167ee8b84fdd39c8fbc57600b37\"}"}

    def test_read_config(self):
        """
        Vérifie la bonne lecture du fichier Json d'exemple et confire que les documents
        ont bien été insérés dans un type de donnée list
        :return:
        """
        assert type(self.parcer.hits) is list

    def test_mendatory_key(self):
        """
        La configuration spécifie une clef obligatoire. "information" (sur kyc 2015)
        les documents qui n'ont pas cette clef sont rejetés du parser.
        On évite les KeyError Exceptions
        :return:
        """
        error_doc = filter(lambda y: "info" not in y.keys(), self.parcer.hits)
        self.assertEquals(0, len(error_doc))

    def test_dump_sting(self):
        """
        Test la convertion string -> dict.
        Rappel: elasticsearch introduit des \" & \n génant dans la version préscédante
        :return:
        """
        Parser.dump_string("{\"hello\": \"world\"}")
        Parser.dump_string('{"hello": "world"}')

    def test_digin_to(self):
        primary = self.parcer.conf.get["primary"]
        assert type(self.hit["info"]) is str
        info = Parser.dump_string(self.hit["info"])
        res = Parser.diginto(info)
        self.assertEquals(15,len(res))
        #print "La méthode de récupération de clefs: (diginto) est appliquée sur la clef obligatoire:",primary
        #print "Le parseur crée à partir du fichier %s, contient %i hits"%(self.parcer.targetfile,len(self.parcer.hits))

    def test_match_pattern(self):
        dictOfLine= Parser.dump_string(self.hit["info"])
        keyValues = Parser.diginto(dictOfLine)
        self.assertEquals("users", self.parcer.matchPattern(keyValues))
        self.assertNotEquals("clics", self.parcer.matchPattern(keyValues))

    def test_restrict_pattern(self):
        content = [{"id":"X"},{"id1":"X"},{"id2":"X"},{"id3":"X"},{"page":"abc"},{"timestamp":"123"},{"type":"B+"}]
        content2 = [{'id': 'X'}, {'page': 'abc'}, {'timestamp': '123'}]
        content = self.parcer.restrictToPattern("users", content)
        content2 = self.parcer.restrictToPattern("users", content2)
        self.assertEqual(len(self.config["files"]["users"]["keys"]), len(content))

    def test_process(self):
        self.parcer.process()


if __name__ == '__main__':
    unittest.main()
