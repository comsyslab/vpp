import logging
import threading
import time
import datetime
from vpp.config.config_ini_parser import ConfigIniParser
from vpp.core.abstract_process import AbstractVPPProcess
from vpp.database.db_maintenance import DBMaintenance


class DBMaintenanceProcess(AbstractVPPProcess):
    def start(self):
        self.lock = threading.Lock()

        self.init()

        self.keep_running = True
        self.run_maintenance_task()

    def init(self):
        ini_parser = ConfigIniParser()

        partition_period_hours = ini_parser.get_measurement_partition_period()
        self.partition_period_seconds = int(datetime.timedelta(hours=partition_period_hours).total_seconds())

        ini_parser = ConfigIniParser()
        window_days = ini_parser.get_rolling_window_length()

        self.maint_impl = DBMaintenance(window_days)

    def run_maintenance_task(self):
        self.last_run = time.time()
        self.maint_impl.do_maintenance()
        time_spent = time.time() - self.last_run
        time_to_next_run = self.partition_period_seconds - time_spent

        self.lock.acquire()
        if self.keep_running:
            self.timer = threading.Timer(time_to_next_run, self.run_maintenance_task)
            self.timer.start()
        self.lock.release()

    def stop(self):
        self.lock.acquire()
        self.keep_running = False
        self.timer.cancel()
        self.lock.release()