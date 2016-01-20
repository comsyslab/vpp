import logging
import multiprocessing
import threading
import time
from abc import ABCMeta, abstractmethod
from enum import Enum

from vpp.core.queue_handler import QueueHandler
from vpp.util import util


class AbstractVPPProcess(object):

    __metaclass__ = ABCMeta

    Commands = Enum('Commands', 'STOP', 'START')

    def __init__(self, in_queue, log_queue=None):
        self.logger = logging.getLogger(__name__)
        self.in_queue = in_queue
        self.log_queue = log_queue
        self.command = None

        self.setup_log_handler()
        self.start()
        self.listen_for_commands()


    def listen_for_commands(self):
        while self.command is None or self.command.index != self.Commands.STOP.index:
            self.logger.debug("Process " + __name__ + " awaiting command")
            self.command = self.in_queue.get()
            self.logger.debug(multiprocessing.current_process().name + " received command " + str(self.command))
        self.stop()

    def setup_log_handler(self):
        handler = QueueHandler(self.log_queue)
        root_logger = logging.getLogger()
        root_logger.handlers = []
        root_logger.addHandler(handler)

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

