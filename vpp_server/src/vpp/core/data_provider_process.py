import logging

from enum import Enum


from vpp.data_acquisition.data_provider import DataProvider
from vpp.database.db_manager import DBManager

class Commands(Enum):
    STOP = 1

class DataProviderProcess(object):

    def __init__(self, in_queue):
        self.logger = logging.getLogger(__name__)
        self.in_queue = in_queue
        self.command = None
        self.db_manager = DBManager()
        self._init_data_providers()
        self._run_data_providers()
        self._listen_for_commands()

    def _init_data_providers(self):
        self.data_providers = []
        for data_provider_entity in self.db_manager.get_data_providers():
            data_provider = DataProvider(data_provider_entity)
            self.data_providers.append(data_provider)

    def _run_data_providers(self):
        for data_provider in self.data_providers:
            self.logger.info("Starting DataProvider %d", data_provider.get_id())
            data_provider.run()

    def _listen_for_commands(self):
        while self.command is not Commands.STOP:
            self.command = self.in_queue.get()

        self._stop_data_providers()
        self.logger.info("DataProviderProcess exiting")

    def _stop_data_providers(self):
        for data_provider in self.data_providers:
            data_provider.stop_process()

        self.wait_for_data_providers_to_exit()

    def wait_for_data_providers_to_exit(self):
        for data_provider in self.data_providers:
            data_provider.join()