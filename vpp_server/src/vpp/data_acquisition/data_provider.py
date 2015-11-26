import logging
import threading
import time

from vpp.core import domain_object_factory
from vpp.data_acquisition.data_provider_timer import DataProviderTimer
from vpp.database.db_manager import DBManager


class FetchingDataProvider(object):

    def __init__(self, entity):
        self.logger = logging.getLogger(__name__)
        self.entity = entity
        self.data_adapter = domain_object_factory.get_data_adapter_from_entity(entity.data_adapter_entity)
        self.data_interpreter = domain_object_factory.instantiate_fqn(entity.data_interpreter_domain_type)
        self.processing_strategy = domain_object_factory.instantiate_fqn(entity.processing_strategy_domain_type)

    def get_id(self):
        return self.entity.id

    def start(self):
        self.timer = DataProviderTimer(self)
        self.timer.start()

    def stop(self):
        self.timer.stop()

    def join(self):
        self.timer.join()

    def get_interval(self):
        return self.entity.interval

    def fetch_and_process_data(self, db_manager=None):
        data = self.data_adapter.fetch_data()
        self.processing_strategy.process_data(data, db_manager)


class ListeningDataProvider(object):

    def __init__(self, entity):
        self.logger = logging.getLogger(__name__)
        self.entity = entity
        self.data_adapter = domain_object_factory.get_data_adapter_from_entity(entity.data_adapter_entity)
        self.data_processor = domain_object_factory.get_data_processor_from_entity(self.entity.data_processor_entity)

    def get_id(self):
        return self.entity.id

    def start(self):
        self.thread = threading.Thread(target=self.data_adapter.listen_for_data, args=(self.process_data,), name=__name__)
        self.thread.setDaemon(True)
        self.thread.start()

    def stop(self):
        self.data_adapter.stop()

    def join(self):
        self.logger.debug("Joining data adapter " + str(self.data_adapter) + "...")
        begin = time.time()
        self.thread.join()
        time_spent = time.time() - begin
        self.logger.debug("...joined in "  + str(time_spent))

    def process_data(self, data, db_manager=None):
        self.data_processor.process_data(data, db_manager)
