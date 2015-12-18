import logging
import os

from enum import Enum


from vpp.core import domain_object_factory
from vpp.core.abstract_process import AbstractVPPProcess
from vpp.data_acquisition.data_provider import DataProvider
from vpp.database.db_manager import DBManager
from vpp.util import util


class DataProviderProcess(AbstractVPPProcess):

    def start(self):
        self._init_data_providers()
        self._start_data_providers()

    def _init_data_providers(self):
        util.init_strptime()
        self.data_providers = []
        data_provider_config_dir = '../resources/data_providers'
        for file_name in os.listdir(data_provider_config_dir):
            rel_file_path = data_provider_config_dir + os.sep + file_name
            if os.path.isfile(rel_file_path) and rel_file_path.endswith('.ini'):
                data_provider = DataProvider(rel_file_path)
                self.data_providers.append(data_provider)
        self.logger.debug("DataProviderProcess found " + str(len(self.data_providers)) + " data providers.")

    def _start_data_providers(self):
        for data_provider in self.data_providers:
            self.logger.info("Starting DataProvider " + data_provider.name)
            data_provider.start()

    def stop(self):
        self.logger.debug("DataProviderProcess signalling provider threads to exit")
        for data_provider in self.data_providers:
            data_provider.stop()

        self._wait_for_data_providers_to_exit()

    def _wait_for_data_providers_to_exit(self):
        self.logger.debug("DataProviderProcess waiting for provider threads to exit")
        for data_provider in self.data_providers:
            data_provider.join()
        self.logger.debug("DataProvider threads exited, DataProviderProcess exiting")