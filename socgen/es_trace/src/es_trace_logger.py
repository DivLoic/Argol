# -*- coding: utf-8 -*-
__author__ = 'LoicMDIVAD'

import sys
import os
import logging
import getpass
from datetime import datetime

# RECALL :
# debug < info < warn < error < critical
# LoggerLevels

LOG_FORMAT = '%(asctime)s [ %(levelname)s ] : %(message)s'
PRT_FORMAT = '----------- [ %(levelname)s ] : %(message)s'
FORMATTER = logging.Formatter(PRT_FORMAT)


class TrcLogger():

    def __init__(self, log_path, test):
        self.date_exec = datetime.now().strftime("%Y-%m")
        log_file_name = log_path + '/' + 'trc_BCC_' + self.date_exec + '.log'
        self.touch(log_file_name)  # $ touch file.log
        logging.basicConfig(format=LOG_FORMAT, filename=log_file_name)

        # Writing in file if info
        self.log = logging.getLogger("TRC")
        if not test:
            self.log.setLevel(logging.DEBUG)

            # Writing in console if warining or error
            self.console = logging.StreamHandler()
            self.console.setLevel(logging.INFO)
            self.console.setFormatter(FORMATTER)
            self.log.addHandler(self.console)
        else:
            self.log.setLevel(logging.CRITICAL)


        self.start()

    def touch(self, path):
        with open(path, 'a') as f:
            f.write("")

    def start(self):
        self.delimite()
        self.log.info('Lancement de la commande trc par le user : %s' % getpass.getuser())

    def delimite(self):
        self.log.info('')
        self.log.info('-' * 72)
        self.log.info('')

    def log4Test(self):
        self.log = logging.getLogger("TEST")
        self.log.setLevel(logging.CRITICAL)
        self.log.handlers = []


    @property
    def get(self):
        return self.log
