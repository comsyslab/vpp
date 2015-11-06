import logging

import sys

from vpp.core.data_provider_process_manager import DataProviderProcessManager
from vpp.database import db_manager

__author__ = 'ubbe'


class Coordinator:

    def __init__(self):
        logging.basicConfig(level=logging.INFO,
                            stream=sys.stdout,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%y-%m-%d %H:%M:%S')


        self.db_manager = db_manager.instance

        self.dataprovider_manager = DataProviderProcessManager()



if __name__ == '__main__':
    coordinator = Coordinator()