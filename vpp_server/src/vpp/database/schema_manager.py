import datetime as dt
import logging
import time

import pytz
import tzlocal
from sqlalchemy import Column, Integer, ForeignKey, Float, DateTime, Table, String
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.declarative import declarative_base

from vpp.config.config_ini_parser import ConfigIniParser
from vpp.util import util

__author__ = 'ubbe@eng.au.dk'

DeclarativeBase = declarative_base()


class SchemaManager(object):
    def __init__(self, engine):
        self.logger = logging.getLogger(__name__)
        self.engine = engine
        self.measurement_base_table_name = 'Measurement'
        self.timezone = tzlocal.get_localzone()
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
        #drop base table with cascade
        sql = 'DROP TABLE IF EXISTS \"' + self.measurement_base_table_name + '\" CASCADE;'
        self.engine.execute(sql)

    def create_missing_tables(self):
        DeclarativeBase.metadata.create_all(self.engine)
        self.create_measurement_basetable()
        #self.get_or_create_measurement_subtable(dt.datetime.now(self.timezone))

    def create_measurement_basetable(self):
        self.measurement_base_table = Table(self.measurement_base_table_name,
                                 DeclarativeBase.metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('sensor_id', String, ForeignKey('Sensor.id'), nullable=False),
                                 Column('timestamp', DateTime(timezone=True), nullable=False),
                                 Column('value', String, nullable=False),
                                 extend_existing=True)
        self.measurement_base_table.create(self.engine, checkfirst=True)

    def get_or_create_measurement_subtable(self, timestamp):
        table_name = self.get_partition_table_name(timestamp)
        table = self.lookup_table(table_name)
        if table is None:
            table = self._create_measurement_subtable(timestamp)
        return table

    def drop_measurement_subtable(self, timestamp):
        table_name = self.get_partition_table_name(timestamp)
        self.drop_table(table_name)

    def drop_table(self, table_name):
        sql = 'DROP TABLE IF EXISTS "' + table_name + '";'
        self.logger.debug(util.get_thread_info() + sql)
        self.engine.execute(sql)

    def _create_measurement_subtable(self, timestamp):
        '''Create initial subtable for the present day'''
        part_start, part_end = self._get_partition_boundary_timestamps(timestamp)

        primary_key_sql = 'PRIMARY KEY (id)'

        timestamp_constraint_sql = 'CHECK (timestamp >= \'' + str(part_start) + '\' AND timestamp < \'' + str(part_end) + '\')'
        foreign_key_sql = 'FOREIGN KEY (sensor_id) REFERENCES "Sensor" (id)'

        constraint_sql = primary_key_sql + ', ' + timestamp_constraint_sql + ', ' + foreign_key_sql
        table_name = self.get_partition_table_name(timestamp)

        sql = 'CREATE TABLE IF NOT EXISTS \"' + table_name + '\"' + \
              '(' + constraint_sql + ')' + \
              ' INHERITS (\"' + self.measurement_base_table_name + '\");'

        result_proxy = self.engine.execute(sql)

        result_proxy.close()
        self.logger.info("Created table " + table_name)
        return self.lookup_table(table_name)

    def get_partition_table_name(self, timestamp):
        """
        :param timestamp: datetime with timezone
        """
        part_start, _ = self._get_partition_boundary_timestamps(timestamp)
        part_start_string = self._date_string_rep(part_start)
        return self.measurement_base_table_name + '_' + part_start_string

    def _get_partition_boundary_timestamps(self, timestamp):
        partition_length_seconds = int(dt.timedelta(hours=self.partition_length_hours).total_seconds())

        seconds_since_epoch = self._convert_datetime_to_epoch(timestamp)
        partition_count_since_epoch = int(seconds_since_epoch/partition_length_seconds)

        partition_start_seconds = partition_count_since_epoch * partition_length_seconds

        start_time_utc = dt.datetime.utcfromtimestamp(partition_start_seconds)
        start_time_utc_w_tz= self.timezone_utc.localize(start_time_utc)
        end_time_utc_w_tz = start_time_utc_w_tz + dt.timedelta(seconds=partition_length_seconds)

        return start_time_utc_w_tz, end_time_utc_w_tz

    def _convert_datetime_to_epoch(self, timestamp_w_timezone):
        epoch = dt.datetime(1970,1,1, tzinfo=self.timezone_utc)
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
            return table
        except NoSuchTableError:
            return None


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