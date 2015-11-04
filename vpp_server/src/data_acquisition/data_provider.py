import threading
import time

from data_acquisition.data_adapter import DataAdapter
from data_acquisition.data_interpreter import DataInterpreter

import database

__author__ = 'ubbe'


class DataProvider(object):

    def __init__(self, dataprovider_entity):

        self.entity = dataprovider_entity

        self.data_adapter = DataAdapter(self.entity.data_adapter)
        self.data_interpreter = DataInterpreter(self.entity.data_interpreter)

        self.stop_signal = False

        self.db_manager = database.db_manager.instance


    def run(self):
        if self.stop_signal:
            return

        start_time = time.time()

        self.process_data()



        time_spent = start_time - time.time()
        time_to_next_run = self.entity.interval - time_spent
        self.timer = threading.Timer(time_to_next_run, self.run)

        self.timer.start()


    def process_data(self):
        data = self.data_adapter.fetch_data()
        data_for_db = self.data_interpreter.interpret_data(data)

        print str(self.entity) + " getting data :)"

        #self.db_manager.create_new_measurement(self, sensor_id, timestamp, value)


    def stop(self):
        self.stop_signal = True
        self.timer.cancel()



