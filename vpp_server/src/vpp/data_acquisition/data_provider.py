import logging
import threading

from vpp.core import domain_object_factory
from vpp.data_acquisition.data_provider_timer import DataProviderTimer

__author__ = 'ubbe'


class DataProvider(object):

    def __init__(self, dataprovider_entity):
        self.logger = logging.getLogger(__name__)

        self.entity = dataprovider_entity

        data_processor_entity = self.entity.data_processor_entity
        self.data_processor = domain_object_factory.get_domain_object_from_entity(data_processor_entity)


    def get_id(self):
        return self.entity.id

    def get_interval(self):
        return self.entity.interval

    def start(self):
        if self.entity.interval is None or \
           self.entity.interval == 0:
            name = __name__ + " " + str(self.data_provider.get_id())
            self.thread = threading.Thread(target=self.fetch_and_process_data, name=name)
        else:
            self.thread = DataProviderTimer(self)

        self.thread.start()

    def stop(self):
        self.thread.stop()

    def join(self):
        self.thread.join()

    def fetch_and_process_data(self):
        self.data_processor.fetch_and_process_data()
        self.logger.debug("DataProvider %d fetched and stored data", self.get_id())

