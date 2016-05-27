import datetime as dt
import logging
import time

import pytz
import tzlocal
from sqlalchemy import Column, Integer, ForeignKey, Float, DateTime, Table, String, Time, Interval
from sqlalchemy.engine import default
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import ColumnDefault
from sqlalchemy.sql.sqltypes import Boolean

from vpp.config.config_ini_parser import ConfigIniParser
from vpp.util import util

__author__ = 'ubbe@eng.au.dk'

DeclarativeBase = declarative_base()


class SchemaManager(object):
    def __init__(self, session, engine):
        self.logger = logging.getLogger(__name__)
        self.session = session
        self.engine = engine
        self.measurement_base_table_name = 'Measurement'
        self.prediction_base_table_name = 'Prediction'
        self.timezone_utc = pytz.timezone('UTC')
        self.partition_length_hours = ConfigIniParser().get_measurement_partition_period()

    def recreate_schema(self):
        #self.drop_measurement_tables()
        #DeclarativeBase.metadata.drop_all(self.engine)

        sql = "DROP SCHEMA public CASCADE;" \
              "CREATE SCHEMA public;" \
              "GRANT ALL ON SCHEMA public TO postgres;" \
              "GRANT ALL ON SCHEMA public TO public;" \
              "COMMENT ON SCHEMA public IS 'standard public schema';"
        self.engine.execute(sql)

    def drop_measurement_tables(self):
        self._drop_table_cascading(self.measurement_base_table_name)

    def _drop_table_cascading(self, table_name):
        #drop base table with cascade
        sql = 'DROP TABLE IF EXISTS \"' + table_name + '\" CASCADE;'
        self.session.execute(sql)

    def create_missing_tables(self):
        DeclarativeBase.metadata.create_all(self.engine)
        self.create_measurement_basetable()
        self.create_prediction_basetable()

    def create_measurement_basetable(self):
        self.measurement_base_table = Table(self.measurement_base_table_name,
                                 DeclarativeBase.metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('sensor_id', String, ForeignKey('Sensor.id'), nullable=False),
                                 Column('timestamp', DateTime(timezone=True), nullable=False),
                                 Column('value', String, nullable=False),
                                 Column('exported', Boolean, nullable=False, default=False),
                                 extend_existing=True)
        self.measurement_base_table.create(self.engine, checkfirst=True)

    def create_prediction_basetable(self):
        self.prediction_base_table = Table(self.prediction_base_table_name,
                                           DeclarativeBase.metadata,
                                           Column('id', Integer, primary_key=True),
                                           Column('endpoint_id', String, ForeignKey('PredictionEndpoint.id'), nullable=False),
                                           Column('timestamp', DateTime(timezone=True), nullable=False),
                                           Column('value', String, nullable=False),
                                           Column('time_received', DateTime(timezone=True), nullable=False),
                                           Column('value_interval', Interval),
                                           Column('exported', Boolean, nullable=False, default=False),
                                           extend_existing=True)
        self.prediction_base_table.create(self.engine, checkfirst=True)

    def get_or_create_measurement_subtable(self, timestamp):
        table_name = self.get_partition_subtable_name(self.measurement_base_table_name, timestamp)
        table = self.lookup_table(table_name)
        if table is None:
            table = self._create_measurement_subtable(timestamp)
        return table

    def get_or_create_prediction_subtable(self, timestamp):
        table_name = self.get_partition_subtable_name(self.prediction_base_table_name, timestamp)
        table = self.lookup_table(table_name)
        if table is None:
            table = self._create_prediction_subtable(timestamp)
        return table

    def drop_table(self, table_name):
        sql = 'DROP TABLE IF EXISTS "' + table_name + '";'
        self.logger.debug(util.get_thread_info() + sql)
        self.session.execute(sql)

    def _create_measurement_subtable(self, timestamp):
        table_name = self._create_partition_subtable(self.measurement_base_table_name, timestamp, 'timestamp')
        sql = 'ALTER TABLE"' + table_name + '" ADD FOREIGN KEY (sensor_id) REFERENCES "Sensor" (id) ON DELETE CASCADE;'
        self.session.execute(sql)
        self.session.commit()
        return self.lookup_table(table_name)

    def _create_prediction_subtable(self, timestamp):
        table_name = self._create_partition_subtable(self.prediction_base_table_name, timestamp, 'time_received')
        sql = 'ALTER TABLE "' + table_name + '" ADD FOREIGN KEY (endpoint_id) REFERENCES "PredictionEndpoint" (id) ON DELETE CASCADE;'
        self.session.execute(sql)
        self.session.commit()
        return self.lookup_table(table_name)

    def _create_partition_subtable(self, base_table_name, timestamp, timestamp_name='timestamp'):
        '''Create initial subtable for the present day'''
        part_start, part_end = self._get_partition_boundary_timestamps(timestamp)

        primary_key_sql = 'PRIMARY KEY (id)'
        timestamp_constraint_sql = 'CHECK (' + timestamp_name + ' >= \'' + str(part_start) + '\' AND ' + timestamp_name + ' <= \'' + str(part_end) + '\')'
        constraint_sql = primary_key_sql + ', ' + timestamp_constraint_sql

        table_name = self.get_partition_subtable_name(base_table_name, timestamp)

        sql = 'CREATE TABLE IF NOT EXISTS "' + table_name + '"' + \
              '(' + constraint_sql + ') ' + \
              'INHERITS ("' + base_table_name + '");'

        result_proxy = self.session.execute(sql)

        result_proxy.close()
        self.logger.info("Created table " + table_name)
        return table_name

    def get_prediction_subtable_name(self, timestamp):
        return self.get_partition_subtable_name(self.prediction_base_table_name, timestamp)

    def get_measurement_subtable_name(self, timestamp):
        return self.get_partition_subtable_name(self.measurement_base_table_name, timestamp)

    def get_partition_subtable_name(self, base_table_name, timestamp):
        """
        :param timestamp: datetime with timezone
        """
        part_start, _ = self._get_partition_boundary_timestamps(timestamp)
        part_start_string = self._date_string_rep(part_start)
        return base_table_name + '_' + part_start_string

    def _get_partition_boundary_timestamps(self, timestamp):
        partition_length_seconds = int(dt.timedelta(hours=self.partition_length_hours).total_seconds())

        seconds_since_epoch = self._convert_datetime_to_time_since_epoch(timestamp)
        partition_count_since_epoch = int(seconds_since_epoch/partition_length_seconds)

        partition_start_seconds = partition_count_since_epoch * partition_length_seconds

        start_time_utc = dt.datetime.utcfromtimestamp(partition_start_seconds)
        start_time_utc_w_tz= self.timezone_utc.localize(start_time_utc)
        end_time_utc_w_tz = start_time_utc_w_tz + dt.timedelta(seconds=partition_length_seconds)

        return start_time_utc_w_tz, end_time_utc_w_tz

    def _convert_datetime_to_time_since_epoch(self, timestamp_w_timezone):
        epoch = util.get_base_date(tzinfo=self.timezone_utc)
        seconds = (timestamp_w_timezone - epoch).total_seconds()
        return seconds

    def _date_string_rep(self, datetime):
        str_rep = str(datetime)
        str_rep_tz_stripped = str_rep[:len(str_rep)-6]
        return self._replace_with_underscores(str_rep_tz_stripped)

    def _replace_with_underscores(self, string):
        return string.replace(' ', '_').replace('-', '_').replace(':', '_').replace('+', '_')

    def lookup_table(self, name):
        try:
            table = Table(name, DeclarativeBase.metadata, autoload=True, autoload_with=self.engine)
            if not table.exists(bind=self.engine):
                table = None
        except NoSuchTableError:
            table = None

        return table


class Measurement(object):
    '''not in use'''
    def __init__(self, id, sensor_id, timestamp, value):
        self.id = id
        self.sensor_id = sensor_id
        self.timestamp = timestamp
        self.value = value

    def __repr__(self):
        return "<Measurement(id='%s', sensor_id='%s', timestamp='%s', value='%s')>" \
                % (self.id, self.sensor_id, self.timestamp, self.value)