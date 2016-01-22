import logging
import threading
import time


class DataProviderTimer(object):

    def __init__(self, fetching_adapter):
        self.logger = logging.getLogger(__name__)
        self.check_stop_interval = 5
        self.data_adapter = fetching_adapter
        self.fetch_interval = fetching_adapter.get_interval()
        self.stop_signal = threading.Event()
        self.thread = None
        self.last_fetch = 0

    def start(self):
        name = __name__ + " " + str(self.data_adapter.data_provider.name)
        self.stop()
        self.stop_signal = threading.Event()
        self.logger.debug("DataProvider %s reset stop signal and started timer thread", self.data_adapter.data_provider.name)
        self.thread = threading.Thread(target=self._run, name=name)
        self.thread.setDaemon(False)
        self.thread.start()

    def _run(self):
        while not self.stop_signal.isSet():
            time_since_fetch = time.time() - self.last_fetch
            if time_since_fetch >= self.fetch_interval:
                self.last_fetch = time.time()
                try:
                    self.data_adapter.fetch_and_process_data()
                except Exception as e:
                    self.logger.exception('Exception during fetch and processing of data: ' + e.message)

            time_until_fetch = self.last_fetch + self.fetch_interval - time.time()
            sleep_time = min(self.check_stop_interval, time_until_fetch)
            threading._sleep(sleep_time)

        self.logger.debug("DataProvider %s exiting due to stop signal", self.data_adapter.data_provider.name)

    def stop(self):
        self.logger.debug("DataProvider %s received stop signal", self.data_adapter.data_provider.name)
        self.stop_signal.set()

    def join(self):
        self.thread.join()

if __name__ == '__main__':
    sleep_time = 5
    print('Started, going to sleep for ' + str(sleep_time) + ' seconds')
    threading._sleep(5)
    print('resumed, exiting...')
