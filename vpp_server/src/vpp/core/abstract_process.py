import logging
import multiprocessing
import threading
import time
from abc import ABCMeta, abstractmethod
from enum import Enum

from vpp.util import util


class AbstractVPPProcess(object):

    __metaclass__ = ABCMeta

    Commands = Enum('Commands', 'STOP', 'START')

    def __init__(self, in_queue):
        self.logger = logging.getLogger(__name__)
        self.in_queue = in_queue
        self.command = None
        self.start()
        self.run_check_log_level_thread()
        self.listen_for_commands()


    def listen_for_commands(self):
        while self.command is None or self.command.index != self.Commands.STOP.index:
            self.logger.debug("Process " + __name__ + " awaiting command")
            self.command = self.in_queue.get()
            self.logger.debug(multiprocessing.current_process().name + " received command " + str(self.command))
        self.stop()

    def run_check_log_level_thread(self):
        thread = threading.Thread(target=self._periodically_check_log_level)
        thread.setDaemon(True)
        thread.start()
        self.logger.info("Started log level thread")

    def _periodically_check_log_level(self):
        while True:
            time.sleep(30)
            util.load_and_set_log_level()

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

