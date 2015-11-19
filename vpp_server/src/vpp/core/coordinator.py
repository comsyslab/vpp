import logging
import os
import sys

from vpp.core.data_provider_process_manager import DataProviderProcessManager
from vpp.database import db_manager
from vpp.database.db_manager import DBManager

__author__ = 'ubbe'


class Coordinator:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.run_data_provider_manager()


    def run_data_provider_manager(self):

       self.dataprovider_process_manager = DataProviderProcessManager()
       self.dataprovider_process_manager.start_process()

       self.logger.info("TestCoordinator exiting")


def init_logging():
    log_file_name = '../../../logs/console.log'

    print "Output is sent to " + os.path.abspath(log_file_name)
    logging.basicConfig(level=logging.INFO,
                        # stream=sys.stdout,
                        filename=log_file_name,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    init_logging()

    coordinator = Coordinator()