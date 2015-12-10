import threading

from vpp.core.abstract_process import AbstractVPPProcess
from vpp.util import util

__author__ = 'ubbe'

import logging
from multiprocessing import Process, Queue


class ProcessWrapper(object):

    def __init__(self, process_class):
        self.logger = logging.getLogger(__name__)
        self.process_class = process_class

    def start(self):
        self.out_queue = Queue()
        self.process = Process(name=self.process_class.__name__, target=self.process_class, args=(self.out_queue,))
        self.process.start()

    def stop(self):
        self.out_queue.put(AbstractVPPProcess.Commands.STOP)
        self.logger.debug(util.get_thread_info() + "ProcessManager sent stop to " + self.process.name)
        self.out_queue.close()
        thread = threading.Thread(target=self._kill_after_timeout)
        thread.setDaemon(False)
        thread.start()

    def _kill_after_timeout(self):
        timeout = 15
        self.logger.debug(util.get_thread_info() + "Waiting for process " + self.process.name + " to exit (timeout " + str(timeout) + ")")
        self.process.join(timeout=timeout)

        if (self.process.is_alive()):
            self.process.terminate()
            self.logger.info(util.get_thread_info() + "Timeout exceeded, forcefully terminating process " + self.process.name)
        else:
            self.logger.info(util.get_thread_info() + self.process.name + " exited gracefully")

    def join(self):
        self.logger.debug(util.get_thread_info() + "Joining process " + self.process.name)
        self.process.join()
        self.logger.debug(util.get_thread_info() + "Joined " + self.process.name)

