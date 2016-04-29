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
        self.keep_running = True
        self.run_maintenance_task()

    def run_maintenance_task(self):
        self.logger.info("DBMaintenance running")
        last_run = time.time()
        DBMaintenance().do_maintenance()
        time_spent = time.time() - last_run


        time_to_next_run = self.get_part_period_secs() - time_spent

        log_string = "DBMaintenance finished in %.2f secs. " % time_spent
        self.lock.acquire()
        if self.keep_running:
            time_to_next_run_hours = time_to_next_run / 3600
            log_string +=  " Next run in %.1f hours." % time_to_next_run_hours
            self.timer = threading.Timer(time_to_next_run, self.run_maintenance_task)
            self.timer.start()
        self.lock.release()

        self.logger.info(log_string)

    def stop(self):
        self.lock.acquire()
        self.keep_running = False
        self.timer.cancel()
        self.lock.release()

    def get_part_period_secs(self):
        partition_period_hours = ConfigIniParser().get_measurement_partition_period()
        return int(datetime.timedelta(hours=partition_period_hours).total_seconds())