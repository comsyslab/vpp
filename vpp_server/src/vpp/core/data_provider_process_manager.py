__author__ = 'ubbe'

import logging
from multiprocessing import Process, Queue

from vpp.data_acquisition.data_provider_process import DataProviderProcess


class DataProviderProcessManager(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def start_process(self):
        self.out_queue = Queue()

        self.process = Process(name="DataProviderProcess", target=DataProviderProcess, args=(self.out_queue,))
        self.process.start()

    def stop_process(self):
        self.out_queue.put(DataProviderProcess.Commands.STOP)
        self.logger.info("DataProviderProcessManager signalled process to stop")
        self.process.join(timeout=15)
        if (self.process.is_alive()):
            self.logger.info("Timeout exceeded, forcefully terminating DataProviderProcess")
            self.process.terminate()
        self.logger.info("DataProviderProcess terminated")


