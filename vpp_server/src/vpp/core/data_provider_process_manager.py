__author__ = 'ubbe'

import logging
from enum import Enum
from multiprocessing import Process, Queue
from vpp.core.data_provider_process import DataProviderProcess, Commands


class DataProviderProcessManager(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def start_process(self):
        self.out_queue = Queue()

        self.process = Process(name="DataProviderManager", target=DataProviderProcess, args=(self.out_queue,))
        self.process.start()

    def stop_process(self):
        self.out_queue.put(Commands.STOP)
        self.process.join()
        self.logger.info("DataProviderProcess quit gracefully")


