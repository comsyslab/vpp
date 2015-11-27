import logging
import threading
import time
from abc import ABCMeta, abstractmethod

from vpp.data_acquisition.data_provider_timer import DataProviderTimer


class AbstractDataAdapter(object):

    __metaclass__ = ABCMeta

    def __init__(self, entity, data_processor):
        self.logger = logging.getLogger(__name__)
        self.entity = entity
        self.data_processor = data_processor

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def join(self):
        pass


class AbstractListeningAdapter(AbstractDataAdapter):

    __metaclass__ = ABCMeta

    def start(self):
        self.thread = threading.Thread(target=self._listen_for_data, args=(), name=__name__)
        self.thread.setDaemon(True)
        self.thread.start()

    def stop(self):
        self.channel.basic_cancel(self.consumer_tag)
        self.channel.stop_consuming()
        self.logger.debug("Listening adapter " + str(self.entity.id) + " cancelled message consumption.")

    def join(self):
        self.logger.debug("Joining data adapter " + str(self.data_adapter) + "...")
        begin = time.time()
        self.thread.join()
        time_spent = time.time() - begin
        self.logger.debug("...joined in "  + str(time_spent))

    @abstractmethod
    def _listen_for_data(self):
        pass


class AbstractFetchingAdapter(AbstractDataAdapter):

    __metaclass__ = ABCMeta

    def get_interval(self):
        return self.entity.interval

    def start(self):
        self.timer = DataProviderTimer(self)
        self.timer.start()

    def stop(self):
        self.timer.stop()

    def join(self):
        self.timer.join()

    def fetch_and_process_data(self, db_manager=None):
        data = self.data_adapter.fetch_data()
        self.data_processor.interpret_and_process_data(data, db_manager)

    @abstractmethod
    def fetch_data(self):
        pass