import logging
import threading
import time


class DataProviderTimer(object):

    def __init__(self, data_provider):
        self.logger = logging.getLogger(__name__)
        self.check_stop_interval = 5
        self.data_provider = data_provider
        self.fetch_interval = data_provider.entity.interval
        self.stop_signal = threading.Event()
        self.thread = None
        self.last_fetch = 0

    def run(self):
        name = __name__ + " " + str(self.data_provider.get_id())
        self.stop()
        self.stop_signal = threading.Event()
        self.thread = threading.Thread(target=self._run, name=name)
        self.thread.setDaemon(False)
        self.thread.start()

    def _run(self):
        while not self.stop_signal.isSet():
            time_since_fetch = time.time() - self.last_fetch
            if  time_since_fetch >= self.fetch_interval:
                self.last_fetch = time.time()
                self.data_provider.fetch_and_process_data()


            time_until_fetch = self.last_fetch + self.fetch_interval - time.time()
            sleep_time = min(self.check_stop_interval, time_until_fetch)
            threading._sleep(sleep_time)

        self.logger.debug("DataProvider %d exiting due to stop signal", self.data_provider.get_id())

    def stop(self):
        self.logger.info("DataProvider %d received stop signal", self.data_provider.get_id())
        self.stop_signal.set()

    def join(self):
        self.thread.join()