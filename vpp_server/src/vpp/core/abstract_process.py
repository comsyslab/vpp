import logging
import multiprocessing
import os
import threading
from abc import ABCMeta, abstractmethod

from enum import Enum


from vpp.core import domain_object_factory
from vpp.data_acquisition.data_provider import DataProvider
from vpp.database.db_manager import DBManager



class AbstractVPPProcess(object):

    __metaclass__ = ABCMeta

    Commands = Enum('Commands', 'STOP', 'START')

    def __init__(self, in_queue):
        self.logger = logging.getLogger(__name__)
        self.in_queue = in_queue
        self.command = None
        self.start()
        self.listen_for_commands()

    def listen_for_commands(self):
        while self.command is None or self.command.index != self.Commands.STOP.index:
            self.logger.debug("Process " + __name__ + " awaiting command")
            self.command = self.in_queue.get()
            self.logger.debug(multiprocessing.current_process().name + " received command " + str(self.command))
        self.stop()


    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

