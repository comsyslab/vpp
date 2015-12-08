import logging
import os
import time
from logging.handlers import RotatingFileHandler

from vpp.core.data_provider_process_manager import DataProviderProcessManager

__author__ = 'ubbe'


class Coordinator:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.start_processes()
        self.check_for_stop()

    def start_processes(self):
       #data providers
       self.dataprovider_process_manager = DataProviderProcessManager()
       self.dataprovider_process_manager.start_process()

        #more to come
        # ...


    def check_for_stop(self):
        stop_file_name = "stop"
        while not os.path.isfile(stop_file_name):
            time.sleep(5)
        self.stop_processes()
        self.logger.info("Coordinator exiting.")
        os.remove(stop_file_name)

    def stop_processes(self):
        self.dataprovider_process_manager.stop_process()


def init_logging():
    log_file_name = '../logs/console.log'

    print "Output is sent to " + os.path.abspath(log_file_name)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(log_file_name, maxBytes=5242880, backupCount=10)
    formatter = logging.Formatter(fmt='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)

    root_logger.addHandler(handler)


if __name__ == '__main__':
    print "Starting VPP server..."
    init_logging()
    coordinator = Coordinator()