import datetime as dt
import time

import pytz
import tzlocal
from sqlalchemy import Column, Integer, ForeignKey, Float, DateTime, Table
from sqlalchemy.ext.declarative import declarative_base

__author__ = 'ubbe@eng.au.dk'

DeclarativeBase = declarative_base()


class TableManager(object):
    def __init__(self, engine):
        self.engine = engine
        self.measurement_base_table_name = 'Measurement'
        self.timezone = tzlocal.get_localzone()

    def drop_tables(self):
        self.drop_measurement_tables()
        DeclarativeBase.metadata.drop_all(self.engine)

    def drop_measurement_tables(self):
        #drop base table with cascade
        sql = 'DROP TABLE IF EXISTS \"' + self.measurement_base_table_name + '\" CASCADE;'
        self.engine.execute(sql)

    def create_missing_tables(self):
        DeclarativeBase.metadata.create_all(self.engine)
        self.create_measurement_basetable()
        self.create_measurement_subtable()


    def create_measurement_basetable(self):
        self.measurement_base_table = Table(self.measurement_base_table_name,
                                 DeclarativeBase.metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('sensor_id', Integer, ForeignKey('Sensor.id'), nullable=False),
                                 Column('timestamp', DateTime(timezone=True), nullable=False),
                                 Column('value', Float, nullable=False),
                                 extend_existing=True)
        self.measurement_base_table.create(self.engine, checkfirst=True)

    def create_measurement_subtable(self):
        '''Create initial subtable for the present day'''
        part_start, part_end = self._get_partition_boundary_timestamps(dt.datetime.now(self.timezone))

        primary_key_sql = 'PRIMARY KEY (id)'
        check_sql= 'CHECK (timestamp >= \'' + str(part_start) + '\' AND timestamp < \'' + str(part_end) + '\')'
        constraint_sql = str(primary_key_sql) + ', ' + str(check_sql)

        table_name = self.get_partition_table_name(dt.datetime.now(self.timezone))

        sql = 'CREATE TABLE IF NOT EXISTS \"' + table_name + '\"' + \
              '(' + constraint_sql + ')' + \
              ' INHERITS (\"' + self.measurement_base_table_name + '\");'

        result_proxy = self.engine.execute(sql)
        result_proxy.close()

    def get_partition_table_name(self, timestamp):
        """
        :param timestamp: datetime with timezone
        """
        part_start, _ = self._get_partition_boundary_timestamps(timestamp)
        part_start_string = self._replace_with_underscores(str(part_start))
        return self.measurement_base_table_name + '_' + part_start_string

    def _get_partition_boundary_timestamps(self, timestamp):

        partition_length_days = 1
        partition_length_seconds = int(dt.timedelta(days=partition_length_days).total_seconds())

        seconds_since_epoch = self._convert_datetime_to_epoch(timestamp)
        partition_count_since_epoch = int(seconds_since_epoch/partition_length_seconds)

        partition_start_seconds = partition_count_since_epoch * partition_length_seconds
        partition_start_datetime = dt.datetime.utcfromtimestamp(partition_start_seconds)

        tzinfo = tzlocal.get_localzone()
        partition_start_datetime = tzinfo.localize(partition_start_datetime)

        partition_end_datetime = partition_start_datetime + dt.timedelta(seconds=partition_length_seconds)

        return partition_start_datetime, partition_end_datetime

    def _convert_datetime_to_epoch(self, timestamp_w_timezone):
        return (timestamp_w_timezone - dt.datetime(1970, 1, 1, tzinfo=self.timezone)).total_seconds()

    def _replace_with_underscores(self, string):
        return string.replace(' ', '_').replace('-', '_').replace(':', '_').replace('+', '_')


    def lookup_table(self, name):
        return Table(name, DeclarativeBase.metadata, autoload=True, autoload_with=self.engine)


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