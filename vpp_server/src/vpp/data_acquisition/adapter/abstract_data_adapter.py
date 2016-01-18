import logging
import threading
import time
from abc import ABCMeta, abstractmethod

from vpp.data_acquisition.adapter.file_date_helper import FileDateHelper
from vpp.data_acquisition.data_provider_timer import DataProviderTimer


class AbstractDataAdapter(object):

    __metaclass__ = ABCMeta

    def __init__(self, data_provider):
        self.logger = logging.getLogger(__name__)
        self.data_provider = data_provider

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

    def __init__(self, data_provider):
        super(AbstractListeningAdapter, self).__init__(data_provider)
        self.retry_delay_short = 3
        self.retry_delay_long = 60

    def start(self):
        self.thread = threading.Thread(target=self._listen_for_data, args=(), name=__name__)
        self.thread.setDaemon(True)
        self.thread.start()

    def join(self):
        self.logger.debug("Joining data adapter " + str(__name__) + "...")
        begin = time.time()
        self.thread.join()
        time_spent = time.time() - begin
        self.logger.debug("...joined in "  + str(time_spent))

    @abstractmethod
    def _listen_for_data(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    def _schedule_reconnect(self):
        threading.Timer(self.retry_delay_long, self._listen_for_data).start()


class AbstractFetchingAdapter(AbstractDataAdapter):

    __metaclass__ = ABCMeta

    def __init__(self, data_provider, data_provider_config):
        super(AbstractFetchingAdapter, self).__init__(data_provider)
        self.config = data_provider_config
        self.file_date_helper = FileDateHelper(data_provider_config.ftp_config)

    def get_interval(self):
        return int(self.config.ftp_config.interval)

    def start(self):
        self.timer = DataProviderTimer(self)
        self.timer.start()

    def stop(self):
        self.timer.stop()

    def join(self):
        self.timer.join()

    def fetch_and_process_data(self, db_manager=None):
        data_strings = self.fetch_data()
        self.data_provider.interpret_and_process_data(data_strings, db_manager)

    @abstractmethod
    def fetch_data(self):
        pass

