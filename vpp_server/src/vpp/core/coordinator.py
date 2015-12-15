import logging
import os
import threading
import time
from logging.handlers import RotatingFileHandler

from vpp.config.config_ini_parser import ConfigIniParser
from vpp.core.process_manager import ProcessWrapper
from vpp.data_acquisition.data_provider_process import DataProviderProcess
from vpp.database.db_maintainer_process import DBMaintainerProcess
from vpp.util import util

__author__ = 'ubbe'


class Coordinator:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.init_processes()
        self.start_processes()
        self.check_for_stop()


    def init_processes(self):
        self.processes = []

        process_data_provider = ProcessWrapper(DataProviderProcess)
        self.processes.append(process_data_provider)

        process_db_maintainer = ProcessWrapper(DBMaintainerProcess)
        #self.processes.append(process_db_maintainer)

    def start_processes(self):
        for process in self.processes:
            process.start()

    def check_for_stop(self):
        stop_file_name = "stop"
        while not os.path.isfile(stop_file_name):
            time.sleep(5)

        self.logger.info(util.get_thread_info() + "File " + stop_file_name + " found, stopping application...")
        self.stop_processes()
        self.wait_for_processes()
        os.remove(stop_file_name)
        self.logger.info(util.get_thread_info() + "Coordinator exiting.")

    def stop_processes(self):
        for process in self.processes:
            process.stop()

    def wait_for_processes(self):
        for process in self.processes:
            process.join()



def init_logging():
    log_file_name = '../logs/console.log'

    print "Output is sent to " + os.path.abspath(log_file_name)

    root_logger = logging.getLogger()
    root_logger.setLevel(ConfigIniParser().get_log_level())

    handler = RotatingFileHandler(log_file_name, maxBytes=5242880, backupCount=10)
    formatter = logging.Formatter(fmt='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)

    root_logger.addHandler(handler)


if __name__ == '__main__':
    print "Starting VPP server..."
    init_logging()
    coordinator = Coordinator()