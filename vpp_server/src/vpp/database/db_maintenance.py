import datetime
import logging
import time

import tzlocal
from sqlalchemy.ext.declarative import declarative_base

from vpp.config.config_ini_parser import ConfigIniParser
from vpp.config.data_provider_config import DataProviderConfig
from vpp.database.db_manager import DBManager
from vpp.util import util


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
        self._create_round_func()
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
        for i in range (0, len(tables)):
            self._copy_table_contents_slow(tables[i])

    def _transfer_subtables_to_dw(self, partition_timestamp):
        if not self.dw_enabled:
            return

        meas_table_name = self.db_manager_local.schema_manager.get_measurement_subtable_name(partition_timestamp)
        pred_table_name = self.db_manager_local.schema_manager.get_prediction_subtable_name(partition_timestamp)

        if self._table_exists_locally(meas_table_name):
            self.db_manager_dw.schema_manager.get_or_create_measurement_subtable(partition_timestamp)
            self._copy_meas_table(meas_table_name)

        if self._table_exists_locally(pred_table_name):
            self.db_manager_dw.schema_manager.get_or_create_prediction_subtable(partition_timestamp)
            self._copy_pred_table(pred_table_name)

        self.db_manager_dw.commit()

    def _drop_for_timestamp(self, timestamp):
        self.logger.debug("DBMaintenance dropping subtables for time " + str(timestamp))
        self.db_manager_local.commit()  # Necessary to prevent DROP TABLE from blocking

        meas_table_name = self.db_manager_local.schema_manager.get_measurement_subtable_name(timestamp)
        pred_table_name = self.db_manager_local.schema_manager.get_prediction_subtable_name(timestamp)

        self._check_all_exported(meas_table_name)
        self._check_all_exported(pred_table_name)

        self.db_manager_local.schema_manager.drop_table(meas_table_name)
        self.db_manager_local.schema_manager.drop_table(pred_table_name)

    def _check_all_exported(self, table_name):
        sql = self._get_count_not_transferred_sql(table_name)
        result = self.db_manager_local.engine.execute(sql).first()
        count = result[0]
        if count > 0:
            self.logger.warning("Dropping table " + table_name + " which contains " +
                                str(count) + " entries that have not been exported to data warehouse.")

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
            try:
                self.db_manager_dw.session.merge(NewRecord(**data))
            except Exception as e:
                self.logger.exception("Exception while merging data into DW: " + str(e.message))
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

    def _copy_meas_table(self, table_name):
        columns = 'sensor_id, timestamp , value, exported'
        columns_w_types = 'sensor_id character varying, ' \
                          'timestamp timestamp with time zone, ' \
                          'value character varying, ' \
                          'exported boolean'
        device_id_column = 'sensor_id'

        files = util.get_data_provider_ini_files()
        for ini_file_path in files:
            self.logger.debug("Transferring measurements according to config in file " + ini_file_path)
            data_provider_config = DataProviderConfig(ini_file_path)
            id_patterns_and_queries = self.get_transfer_queries_for_data_provider(columns, data_provider_config, device_id_column, table_name, self._get_meas_averaging_select_where_sql)

            for id_pattern, query in id_patterns_and_queries:
                self._transfer_data_to_dw(table_name, columns, columns_w_types, device_id_column, query, id_pattern)

    def _copy_pred_table(self, table_name):
        columns = 'endpoint_id, timestamp, value, time_received, value_interval, exported'
        columns_w_types = 'endpoint_id character varying, ' \
                          'timestamp timestamp with time zone, ' \
                          'value character varying, ' \
                          'time_received timestamp with time zone, ' \
                          'value_interval interval, ' \
                          'exported boolean'
        device_id_column = 'endpoint_id'

        files = util.get_data_provider_ini_files()
        for ini_file_path in files:
            self.logger.debug("Transferring predictions according to config in file " + ini_file_path)
            data_provider_config = DataProviderConfig(ini_file_path)
            id_patterns_and_queries = self.get_transfer_queries_for_data_provider(columns, data_provider_config, device_id_column, table_name, self._get_pred_averaging_select_where_sql)

            for id_pattern, query in id_patterns_and_queries:
                self._transfer_data_to_dw(table_name, columns, columns_w_types, device_id_column, query, id_pattern)

    def get_transfer_queries_for_data_provider(self, columns, data_provider_config, device_id_column, table_name, avg_query_func):
        avg_config = data_provider_config.averaging_config
        id_patterns_and_queries = []
        if avg_config.enabled:
            for id_pattern, interval in avg_config.id_patterns_and_intervals:
                query_sql = avg_query_func(table_name, interval, id_pattern)
                tuple = (id_pattern, query_sql)
                id_patterns_and_queries.append(tuple)
                self.logger.debug("Added ID pattern " + id_pattern + " and interval " + str(interval))

        device_id_prefix = data_provider_config.id_prefix
        id_pattern = device_id_prefix + '%%'
        query_sql = self._get_simple_select_where_sql(table_name, columns, device_id_column, id_pattern)
        self.logger.debug("Added default ID pattern " + id_pattern)
        tuple = (id_pattern, query_sql)
        id_patterns_and_queries.append(tuple)

        return id_patterns_and_queries

    def _get_meas_averaging_select_where_sql(self, table_name, averaging_interval, id_pattern):
        return 'SELECT sensor_id, ' \
                      'ts_round( "timestamp", ' + str(averaging_interval) + ' ) AS "timestamp", ' \
                      'CAST(AVG(CAST(value AS float)) AS text) AS value, ' \
                      'FALSE as exported ' \
               'FROM "' + table_name + '" ' \
               'WHERE sensor_id LIKE \'\'' + id_pattern + '\'\' AND exported=FALSE ' \
               'GROUP BY 2, sensor_id ORDER BY sensor_id;'

    def _get_pred_averaging_select_where_sql(self, table_name, averaging_interval_secs, id_pattern):
        return 'SELECT endpoint_id, ' \
                       'ts_round( "timestamp", ' + str(averaging_interval_secs) + ' ) AS "timestamp", ' \
                       'CAST(AVG(CAST(value AS float)) AS text) AS value, ' \
                       'MIN(time_received) AS time_received, ' \
                       '\'\'' + str(averaging_interval_secs) + ' seconds\'\' AS value_interval, ' \
                       'FALSE as exported ' \
                'FROM "' + table_name + '" ' \
                'WHERE endpoint_id LIKE \'\'' + id_pattern + '\'\' AND exported=FALSE ' \
                'GROUP BY 2, endpoint_id ORDER BY endpoint_id;'

    def _get_simple_select_where_sql(self, table_name, columns, id_column, id_pattern):
        return 'SELECT ' + columns + ' FROM "' + table_name + '" WHERE ' + id_column + ' LIKE \'\'' + id_pattern + '\'\' AND exported=FALSE;'

    def _get_update_transferred_sql(self, table_name, device_id_column, id_pattern):
        return 'UPDATE "' + table_name+ '" '\
               'SET exported=TRUE ' \
               'WHERE ' + device_id_column + ' LIKE \'' + id_pattern + '\';'

    def _get_count_not_transferred_sql(self, table_name):
        return 'SELECT COUNT(*) FROM "' + table_name + '" WHERE exported=FALSE;'

    def _transfer_data_to_dw(self, table_name, columns, columns_w_types, device_id_column, query_sql, device_id_prefix):
        count_sql = "SELECT COUNT (*) FROM \""+ table_name +"\";"
        result = self.db_manager_local.engine.execute(count_sql).first()
        count = result[0]

        transfer_sql = "CREATE EXTENSION IF NOT EXISTS dblink; "\
               "INSERT INTO \"" + table_name + "\" (" + columns + ") "\
                   "SELECT * "\
                   "FROM dblink('" + self.db_string_local_dblink + "',"\
                                "'" + query_sql + "') "\
                   "AS temp_table(" + columns_w_types + ");"

        time_start = time.time()

        try:
            transfer_result = self.db_manager_dw.engine.execute(transfer_sql)
            result_count = transfer_result.rowcount
        except Exception as e:
            self.logger.exception("Exception while executing SQL: " + transfer_sql + "\n Exception says: " + str(e.message))
            return

        try:
            update_sql = self._get_update_transferred_sql(table_name, device_id_column, device_id_prefix)
            self.db_manager_local.engine.execute(update_sql)
            self.db_manager_local.commit()
        except Exception as e:
            self.logger.exception("Exception while executing SQL: " + update_sql + "\n Exception says: " + str(e.message))

        time_spent = (time.time() - time_start)
        string = 'From table %s with total %d rows, %d new rows were inserted in DW in %.2f secs' % (table_name, count, result_count, time_spent)
        if result_count > 0:
            self.logger.info(string)
        else:
            self.logger.debug(string)

    def _quick_mapper(self, table):
        Base = declarative_base()
        class GenericMapper(Base):
            __table__ = table
        return GenericMapper

    def _table_exists_locally(self, table_name):
        return self.db_manager_local.schema_manager.lookup_table(table_name) != None

    def _create_round_func(self):
        sql = "CREATE OR REPLACE FUNCTION ts_round( timestamptz, INT4 ) RETURNS TIMESTAMPTZ AS $$"\
              "SELECT 'epoch'::timestamptz + '1 second'::INTERVAL * ( $2 * ( extract( epoch FROM $1 )::INT4 / $2 ) );"\
               "$$ LANGUAGE SQL;"

        try:
            result = self.db_manager_local.engine.execute(sql)
        except Exception as e:
            self.logger.exception("Exception while executing SQL: " + sql + ".\n Exception says: " + str(e.message))

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
