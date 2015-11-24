import logging
import os
import time

from vpp.core.data_provider_process_manager import DataProviderProcessManager

__author__ = 'ubbe'


class Coordinator:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.run_data_provider_manager()
        self.check_for_stop()


    def run_data_provider_manager(self):
       self.dataprovider_process_manager = DataProviderProcessManager()
       self.dataprovider_process_manager.start_process()


    def check_for_stop(self):
        stop_file_name = "stop"
        while not os.path.isfile(stop_file_name):
            time.sleep(5)
        self.dataprovider_process_manager.stop_process()
        self.logger.info("Coordinator exiting.")
        os.remove(stop_file_name)


def init_logging():
    log_file_name = '../logs/console.log'

    print "Output is sent to " + os.path.abspath(log_file_name)
    logging.basicConfig(level=logging.INFO,
                        # stream=sys.stdout,
                        filename=log_file_name,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    init_logging()
    coordinator = Coordinator()