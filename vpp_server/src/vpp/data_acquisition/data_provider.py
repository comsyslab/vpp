import logging
import multiprocessing
import threading
import time

from vpp import database
from vpp.data_acquisition.data_adapter import DataAdapter
from vpp.data_acquisition.data_interpreter import DataInterpreter
from vpp.database.db_manager import DBManager

__author__ = 'ubbe'


class DataProvider(object):

    def __init__(self, dataprovider_entity):

        self.logger = logging.getLogger(__name__)

        self.entity = dataprovider_entity
        self.thread = None
        self.last_fetch = 0

        self.fetch_interval = dataprovider_entity.interval
        self.check_stop_interval = 5

        self.data_adapter = DataAdapter(self.entity.data_adapter)
        self.data_interpreter = DataInterpreter(self.entity.data_interpreter)

        self.stop_signal = threading.Event()
        self.timer = None

        self.db_manager = DBManager()

    def get_id(self):
        return self.entity.id

    def run(self):
        name = __name__ + " " + str(self.get_id())
        self.stop()
        self.stop_signal = threading.Event()
        self.thread = threading.Thread(target=self._run, name=name)
        self.thread.setDaemon(False)
        self.thread.start()

    def _run(self):
        process_name = multiprocessing.current_process().name
        thread_name = threading.current_thread().name

        while not self.stop_signal.isSet():
            time_since_fetch = time.time() - self.last_fetch
            if  time_since_fetch >= self.fetch_interval:
                self.fetch_data()


            time_until_fetch = self.last_fetch + self.fetch_interval - time.time()
            sleep_time = min(self.check_stop_interval, time_until_fetch)

            #self.logger.info("Sleeping for %f seconds", sleep_time )
            threading._sleep(sleep_time)

        self.logger.info("Process %s thread %s DataProvider id %s exited due to stop signal :) ",
                         process_name, thread_name, str(self.entity.id))


    def fetch_data(self):
        data = self.data_adapter.fetch_data()
        data_for_db = self.data_interpreter.interpret_data(data)

        self.last_fetch = time.time()
        process_name = multiprocessing.current_process().name
        thread_name = threading.current_thread().name
        self.logger.info("Process %s thread %s DataProvider id %s getting data :) ",
                         process_name, thread_name, str(self.entity.id))

        #self.db_manager.create_new_measurement(self, sensor_id, timestamp, value)


    def stop(self):
        process_name = multiprocessing.current_process().name
        thread_name = threading.current_thread().name
        self.logger.info("Process %s thread %s DataProvider %d stop called", process_name, thread_name, self.get_id())
        self.stop_signal.set()

    def join(self):
        self.thread.join()


