import logging
import os

from enum import Enum


from vpp.core import domain_object_factory
from vpp.data_acquisition.data_provider import DataProvider
from vpp.database.db_manager import DBManager



class AbstractProcess(object):

    Commands = Enum('Commands', 'STOP', 'START')

    def __init__(self, in_queue):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("DataProviderProcess started")
        self.in_queue = in_queue
        self.command = None
        self.db_manager = DBManager()
        self._init_data_providers()
        self._run_data_providers()
        self._listen_for_commands()



    def _run_data_providers(self):
        for data_provider in self.data_providers:
            self.logger.info("Starting DataProvider " + data_provider.name)
            data_provider.start()

    def _listen_for_commands(self):
        while self.command is None or self.command.index != self.Commands.STOP.index:
            self.logger.debug("DataProviderProcess awaiting command")
            self.command = self.in_queue.get()
            self.logger.debug("DataProviderProcess received command " + str(self.command))

        self._stop()

    def _stop(self):
        self.logger.info("DataProviderProcess signalling provider threads to exit")
        for data_provider in self.data_providers:
            data_provider.stop()

        self._wait_for_data_providers_to_exit()

    def _wait_for_data_providers_to_exit(self):
        self.logger.info("DataProviderProcess waiting for provider threads to exit")
        for data_provider in self.data_providers:
            data_provider.join()
        self.logger.info("DataProvider threads exited, DataProviderProcess exiting")