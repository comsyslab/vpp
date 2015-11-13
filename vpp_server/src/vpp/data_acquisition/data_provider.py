import logging
import threading

from vpp.core import domain_object_factory
from vpp.data_acquisition.data_provider_timer import DataProviderTimer

__author__ = 'ubbe'


class CallbackDataProvider(object):

    def __init__(self, dataprovider_entity):
        self.logger = logging.getLogger(__name__)
        self.entity = dataprovider_entity

        data_processor_entity = self.entity.data_processor_entity
        self.data_processor = domain_object_factory.get_domain_object_from_entity(data_processor_entity)

    def get_id(self):
        return self.entity.id

    def start(self):
        self.data_processor.listen_for_data()

    def stop(self):
        self.data_processor.stop_listening()

    def join(self):
        self.data_processor.join()



class PeriodicDataProvider(object):

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
       self.timer = DataProviderTimer(self)
       self.timer.start()

    def stop(self):
        self.timer.stop()

    def join(self):
        self.timer.join()

    def fetch_and_process_data(self):
        self.data_processor.listen_for_data()
        self.logger.debug("DataProvider %d fetched and stored data", self.get_id())

