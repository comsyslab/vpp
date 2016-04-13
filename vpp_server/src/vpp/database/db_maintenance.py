import datetime
import logging
import time

import tzlocal
from sqlalchemy.ext.declarative import declarative_base

from vpp.config.config_ini_parser import ConfigIniParser
from vpp.database.db_manager import DBManager


class DBMaintenance(object):

    def __init__(self, window_length_days):
        self.logger = logging.getLogger(__name__)
        self.window_seconds = int(datetime.timedelta(days=window_length_days).total_seconds())

    def do_maintenance(self):
        self.db_manager_local = DBManager(autoflush=False)

        #find subtables scheduled for transfer and deletion
        partition_timestamp = datetime.datetime.now(tz=tzlocal.get_localzone()) - datetime.timedelta(seconds=self.window_seconds)

        if ConfigIniParser().get_dw_enabled():
            self._transfer_to_dw(partition_timestamp)

        self.logger.info("DBMaintenance dropping subtables for time " + str(partition_timestamp))
        self.db_manager_local.commit() #Necessary to prevent DROP TABLE from blocking

        meas_table_name = self.db_manager_local.schema_manager.get_measurement_subtable_name(partition_timestamp)
        pred_table_name = self.db_manager_local.schema_manager.get_prediction_subtable_name(partition_timestamp)
        self.db_manager_local.schema_manager.drop_table(meas_table_name)
        self.db_manager_local.schema_manager.drop_table(pred_table_name)

        self.db_manager_local.close()

    def _transfer_to_dw(self, partition_timestamp):
        db_string_dw = ConfigIniParser().get_db_string('DB-DW')
        self.db_manager_dw = DBManager(db_string_dw, autoflush=False)

        # copy regular tables to data warehouse
        self.db_manager_dw.create_missing_tables()
        tables = ['Building', 'FloorSection', 'Room', 'Device', 'Controller', 'Sensor', 'ControlAction',
                  'DeviceLocation', 'PredictionEndpoint']
        for table in tables:
            self._copy_table_contents(table)

        # create subtables in data warehouse
        self.db_manager_dw.schema_manager.get_or_create_measurement_subtable(partition_timestamp)
        self.db_manager_dw.schema_manager.get_or_create_prediction_subtable(partition_timestamp)
        self.db_manager_dw.commit()

        # transfer subtable contents to data warehouse
        meas_table_name = self.db_manager_local.schema_manager.get_measurement_subtable_name(partition_timestamp)
        pred_table_name = self.db_manager_local.schema_manager.get_prediction_subtable_name(partition_timestamp)
        self._copy_table_contents(meas_table_name)
        self._copy_table_contents(pred_table_name)

        self.db_manager_dw.close()

    def _copy_table_contents(self, table_name):
        table_local = self.db_manager_local.schema_manager.lookup_table(table_name)
        if table_local is None:
            self.logger.debug("Could not transfer non-existing table " + table_name + " to DW.")
            return
        NewRecord = self._quick_mapper(table_local)
        columns = table_local.columns.keys()
        count = 0
        time_start = time.time()
        for record in self.db_manager_local.session.query(table_local).all():
            data = dict([(str(column), getattr(record, column)) for column in columns])
            self.db_manager_dw.session.merge(NewRecord(**data))
            count += 1
            if count % 1000 == 0:
                self.db_manager_dw.commit()
                time_spent = time.time() - time_start
                avg_time = (time_spent / count) * 1000
                string = 'Table %s transferred %d entries to DW in %.2f secs, average %.1f ms/entry' % (table_local.name, count, time_spent, avg_time)
                self.logger.debug(string)

        self.db_manager_dw.commit()

        if count > 0:
            time_spent = time.time() - time_start
            avg_time = (time_spent / count) * 1000
            string = 'Table %s transferred total of %d entries to DW in %.2f secs, average %.1f ms/entry' % (table_local.name, count, time_spent, avg_time)
            self.logger.debug(string)

    def _quick_mapper(self, table):
        Base = declarative_base()
        class GenericMapper(Base):
            __table__ = table
        return GenericMapper


if __name__ == '__main__':
    pass
    #partition_timestamp = datetime.datetime(2015, 11, 18, hour=6, tzinfo=tzlocal.get_localzone())
    #Testing
    #maintainer = DBMaintenance()
    #maintainer.do_maintenance()
    #tables = ['Device', 'Sensor', 'Measurement_2016_02_15_00_00_00']
    #tables = ['Measurement']
    #maintainer._copy_table_schema('Measurement')
    #maintainer._copy_table_schema('Measurement_2016_02_15_00_00_00')
    #maintainer._copy_tables(tables)
