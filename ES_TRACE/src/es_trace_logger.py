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

    def __init__(self, log_path):
        self.date_exec = datetime.now().strftime("%Y-%m")
        log_file_name = log_path + '/' + 'trc_BCC_' + self.date_exec + '.log'
        self.touch(log_file_name)  # $ touch file.log
        logging.basicConfig(format=LOG_FORMAT, filename=log_file_name)

        # Writing in file if info
        self.log = logging.getLogger("TRC")
        self.log.setLevel(logging.INFO)

        # Writing in console if warining or error
        self.console = logging.StreamHandler()
        self.console.setLevel(logging.WARNING)
        self.console.setFormatter(FORMATTER)
        self.log.addHandler(self.console)

    def touch(self, path):
        with open(path, 'a') as f:
            f.write("")

    def start(self):
        self.delimite()
        self.log.info('Lancement de la commande trc par le user : %s' % getpass.getuser())

    def log4test(self):
        self.log.setLevel(logging.CRITICAL)

    def delimite(self):
        self.log.info('')
        self.log.info('-' * 72)
        self.log.info('')

    @property
    def get(self):
        return self.log
