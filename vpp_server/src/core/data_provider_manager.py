from multiprocessing import Process

from data_acquisition.data_provider import DataProvider
from database import db_manager

__author__ = 'ubbe'


class DataProviderManager(object):
    def __init__(self):
        self.db_manager = db_manager.instance

        self.data_providers = []

        for data_provider_entity in self.db_manager.get_data_providers():
            data_provider = DataProvider(data_provider_entity)
            self.data_providers.append(data_provider)

    def start_data_provider_threads(self):
        self.process = Process(target=self._start_data_providers())
        self.process.start()

    def _start_data_providers(self):
        for data_provider in self.data_providers:
            data_provider.run()


    def stop_data_providers(self):
        for data_provider in self.data_providers:
            data_provider.stop()

        self.process.join()