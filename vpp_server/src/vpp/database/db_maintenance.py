import datetime
import logging
import time

import tzlocal
from sqlalchemy.ext.declarative import declarative_base

from vpp.config.config_ini_parser import ConfigIniParser
from vpp.database.db_manager import DBManager


class DBMaintenance(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        self.dw_enabled = None
        self.db_string_dw = None
        self.window_seconds = None
        self.partitition_period_seconds = None
        self.db_manager_local = None
        self.db_manager_dw = None

        self.parse_config()

    def parse_config(self):
        ini_parser = ConfigIniParser()
        self.dw_enabled = ini_parser.get_dw_enabled()
        self.db_string_dw = ini_parser.get_db_string('DB-DW')

        self.db_string_dw_dblink = ini_parser.get_db_string_dblink('DB-DW')
        self.db_string_local_dblink = ini_parser.get_db_string_dblink('DB')

        window_length_days = ini_parser.get_rolling_window_length()
        self.window_seconds = int(datetime.timedelta(days=window_length_days).total_seconds())

        partition_period_hours = ini_parser.get_measurement_partition_period()
        self.partitition_period_seconds = int(datetime.timedelta(hours=partition_period_hours).total_seconds())

    def do_maintenance(self):
        self.db_manager_local = DBManager(autoflush=False)
        self.db_manager_dw = DBManager(self.db_string_dw, autoflush=False)

        #transfer
        self._transfer_main_tables_to_dw()

        #find subtables scheduled for transfer and deletion - and search back for any old tables
        for part_back_index in range(0, 150):
            secs_to_look_back = self.window_seconds + part_back_index * self.partitition_period_seconds
            partition_timestamp = datetime.datetime.now(tz=tzlocal.get_localzone()) - datetime.timedelta(seconds=secs_to_look_back)
            self._transfer_subtables_to_dw(partition_timestamp)
            self._drop_for_timestamp(partition_timestamp)

        self.db_manager_local.close()
        self.db_manager_dw.close()


    def _transfer_main_tables_to_dw(self):
        if not self.dw_enabled:
            return

        # copy regular tables to data warehouse
        self.db_manager_dw.create_missing_tables()
        tables = ['Building', 'FloorSection', 'Room', 'Device', 'Controller', 'Sensor', 'ControlAction',
                  'DeviceLocation', 'PredictionEndpoint']
        for table in tables:
            self._copy_table_contents_slow(table)

    def _transfer_subtables_to_dw(self, partition_timestamp):
        if not self.dw_enabled:
            return

        meas_table_name = self.db_manager_local.schema_manager.get_measurement_subtable_name(partition_timestamp)
        pred_table_name = self.db_manager_local.schema_manager.get_prediction_subtable_name(partition_timestamp)

        if self._table_exists_locally(meas_table_name):
            self.db_manager_dw.schema_manager.get_or_create_measurement_subtable(partition_timestamp)
            self._copy_meas_table_contents_sql(meas_table_name)

        if self._table_exists_locally(pred_table_name):
            self.db_manager_dw.schema_manager.get_or_create_prediction_subtable(partition_timestamp)
            self._copy_pred_table_contents_sql(pred_table_name)

        self.db_manager_dw.commit()

    def _drop_for_timestamp(self, timestamp):
        self.logger.info("DBMaintenance dropping subtables for time " + str(timestamp))
        self.db_manager_local.commit()  # Necessary to prevent DROP TABLE from blocking

        meas_table_name = self.db_manager_local.schema_manager.get_measurement_subtable_name(timestamp)
        pred_table_name = self.db_manager_local.schema_manager.get_prediction_subtable_name(timestamp)
        self.db_manager_local.schema_manager.drop_table(meas_table_name)
        self.db_manager_local.schema_manager.drop_table(pred_table_name)

    def _copy_table_contents_slow(self, table_name):
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
            self.logger.info(string)

    def _copy_meas_table_contents_sql(self, table_name):
        columns = 'sensor_id, timestamp , value'
        columns_w_types = 'sensor_id character varying, timestamp timestamp with time zone, value character varying'
        self._copy_table_contents_sql(table_name, columns, columns_w_types)

    def _copy_pred_table_contents_sql(self, table_name):
        columns = 'endpoint_id, timestamp, value, time_received, value_interval'
        columns_w_types = 'endpoint_id character varying, timestamp timestamp with time zone, value character varying, time_received timestamp with time zone, value_interval interval'
        self._copy_table_contents_sql(table_name, columns, columns_w_types)

    def _copy_table_contents_sql(self, table_name, columns, columns_w_types):
        '''sql_example = "CREATE EXTENSION IF NOT EXISTS dblink;"\
              "SELECT dblink_exec('dbname=vpp port=5432 host=10.24.128.230 user=ubbe password=M1thmpw1', " \
                                 "'CREATE EXTENSION IF NOT EXISTS dblink;"\
                                  "INSERT INTO \"Measurement_2016_04_26_00_00_00\" (sensor_id, timestamp , value)"\
                                      "SELECT *"\
                                      "FROM dblink(''dbname=vpp port=5432 host=cx-14d94a.st.client.au.dk user=vpp password=comsysr00t'',"\
                                                  "''SELECT sensor_id, timestamp , value FROM \"Measurement_2016_04_29_00_00_00\"'')"\
                                      "AS meas(sensor_id character varying, timestamp timestamp with time zone, value character varying)"\
              "');"'''

        sql = "SELECT COUNT (*) FROM \""+ table_name +"\";"
        result = self.db_manager_local.engine.execute(sql).first()
        count = result[0]

        sql = "CREATE EXTENSION IF NOT EXISTS dblink;"\
              "SELECT dblink_exec('" + self.db_string_dw_dblink + "', " \
                                  "'CREATE EXTENSION IF NOT EXISTS dblink; "\
                                   "INSERT INTO \"" + table_name + "\" (" + columns + ") "\
                                       "SELECT * "\
                                       "FROM dblink(''" + self.db_string_local_dblink + "'',"\
                                                    "''SELECT " + columns + " FROM \"" + table_name + "\"'') "\
                                       "AS meas(" + columns_w_types + ") "\
              "');"

        self.logger.debug("Executing " + sql)
        try:
            time_start = time.time()
            result = self.db_manager_local.engine.execute(sql)
            time_spent = (time.time() - time_start) / 1000
            avg_time = time_spent / count
            string = 'Table %s transferred total of %d entries to DW in %.2f secs, average %.1f ms/entry' % (table_name, count, time_spent, avg_time)
            self.logger.info(string)
        except Exception as e:
            self.logger.exception("Exception while executing SQL: " + sql + ".\n Exception says: " + str(e.message))


    def _quick_mapper(self, table):
        Base = declarative_base()
        class GenericMapper(Base):
            __table__ = table
        return GenericMapper

    def _table_exists_locally(self, table_name):
        return self.db_manager_local.schema_manager.lookup_table(table_name) != None

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
