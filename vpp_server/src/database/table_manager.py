import datetime as dt
import time
import pytz
from sqlalchemy.ext.declarative import declarative_base
import tzlocal


import sqlalchemy
from sqlalchemy import Column, Integer, ForeignKey, Float, DateTime, Table


__author__ = 'ubbe@eng.au.dk'

DeclarativeBase = declarative_base()

import core_entities
import dataprovider_entities

class TableManager(object):
    def __init__(self, engine):
        self.engine = engine
        self.measurement_base_table_name = 'Measurement'

    def drop_tables(self):
        self.drop_measurement_tables()
        DeclarativeBase.metadata.drop_all(self.engine)

    def drop_measurement_tables(self):
        #drop base table with cascade
        sql = 'DROP TABLE IF EXISTS \"' + self.measurement_base_table_name + '\" CASCADE;'
        self.engine.execute(sql)

    def create_missing_tables(self):
        self.create_measurement_basetable()
        DeclarativeBase.metadata.create_all(self.engine)
        self.create_measurement_subtable()

    def create_measurement_basetable(self):
        self.measurement_base_table = Table(self.measurement_base_table_name,
                                 DeclarativeBase.metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('sensor_id', Integer, ForeignKey('Sensor.id'), nullable=False),
                                 Column('timestamp', DateTime(timezone=True), nullable=False),
                                 Column('value', Float, nullable=False))

    def create_measurement_subtable(self):
        '''Create initial subtable for the present day'''
        part_start, part_end = self.get_partition_boundary_timestamps()

        primary_key_sql = 'PRIMARY KEY (id)'
        check_sql= 'CHECK (timestamp >= \'' + str(part_start) + '\' AND timestamp < \'' + str(part_end) + '\')'
        constraint_sql = str(primary_key_sql) + ', ' + str(check_sql)

        table_name = self.get_partition_table_name(dt.datetime.now())

        sql = 'CREATE TABLE IF NOT EXISTS \"' + table_name + '\"' + \
              '(' + constraint_sql + ')' + \
              ' INHERITS (\"' + self.measurement_base_table_name + '\");'

        result_proxy = self.engine.execute(sql)
        result_proxy.close()

    def get_partition_table_name(self, timestamp=dt.datetime.now()):
        part_start, _ = self.get_partition_boundary_timestamps(timestamp)
        part_start_string = self.replace_with_underscores(str(part_start))
        return self.measurement_base_table_name + '_' + part_start_string

    def get_partition_boundary_timestamps(self, timestamp=dt.datetime.now()):

        partition_length_days = 1
        partition_length_seconds = int(dt.timedelta(days=partition_length_days).total_seconds())

        seconds_since_epoch = self.convert_datetime_to_epoch(timestamp)
        partition_count_since_epoch = int(seconds_since_epoch/partition_length_seconds)

        partition_start_seconds = partition_count_since_epoch * partition_length_seconds
        partition_start_datetime = dt.datetime.utcfromtimestamp(partition_start_seconds)

        tzinfo = pytz.timezone(str(tzlocal.get_localzone()))
        partition_start_datetime = tzinfo.localize(partition_start_datetime)

        partition_end_datetime = partition_start_datetime + dt.timedelta(seconds=partition_length_seconds)

        return partition_start_datetime, partition_end_datetime

    def convert_datetime_to_epoch(self, timestamp):
        format = "%Y-%m-%d %H:%M:%S"
        timestamp_string = str(timestamp)

        if (timestamp_string.find('.') != -1): #strip second decimals
            timestamp_string = timestamp_string[0:timestamp_string.find('.')]

        struct_time = time.strptime(timestamp_string, format)
        return int(time.mktime(struct_time))

    def replace_with_underscores(self, string):
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