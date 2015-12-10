import logging
import threading

import time

import datetime

import tzlocal

from vpp.config.config_ini_parser import ConfigIniParser
from vpp.core.abstract_process import AbstractVPPProcess
from vpp.database.db_manager import DBManager


class DBMaintainerProcess(AbstractVPPProcess):



    def start(self):
        self.lock = threading.Lock()

        self.load_config()

        self.keep_running = True
        self.run_maintenance_task()

    def load_config(self):
        ini_parser = ConfigIniParser()

        window_days = ini_parser.get_rolling_window_length()
        self.window_seconds = int(datetime.timedelta(days=window_days).total_seconds())

        partition_period_hours = ini_parser.get_measurement_partition_period()
        self.partition_period_seconds = int(datetime.timedelta(hours=partition_period_hours).total_seconds())

    def run_maintenance_task(self):
        self.last_run = time.time()
        self._do_maintenance()
        time_spent = time.time() - self.last_run
        time_to_next_run = self.partition_period_seconds - time_spent

        self.lock.acquire()
        if self.keep_running:
            self.timer = threading.Timer(time_to_next_run, self.run_maintenance_task).start()
        self.lock.release()


    def _do_maintenance(self):
        timestamp = datetime.datetime.now(tz=tzlocal.get_localzone()) - datetime.timedelta(seconds=self.window_seconds)
        db_manager = DBManager()
        self.logger.info("DBMaintainerProcess dropping table for time " + str(timestamp))
        db_manager.schema_manager.drop_measurement_subtable(timestamp)
        db_manager.close()


    def stop(self):
        self.lock.acquire()
        self.keep_running = False
        self.timer.cancel()
        self.lock.release()