import datetime as dt
import time

import pytz
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.schema import UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Time, Float, DateTime
import tzlocal

__author__ = 'ubbe@eng.au.dk'

DeclarativeBase = declarative_base()

class TableManager(object):
    def __init__(self, engine):
        self.engine = engine

    def drop_tables(self):
        sql = 'DROP TABLE IF EXISTS ' + self.get_current_partition_table_name() + ';'
        self.engine.execute(sql)
        DeclarativeBase.metadata.drop_all(self.engine)

    def create_missing_tables(self):
        DeclarativeBase.metadata.create_all(self.engine)
        self.create_measurement_subtable()

    def create_measurement_subtable(self):
        '''Create initial subtable for the present day'''
        part_start, part_end = self.get_partition_boundary_timestamps()

        primary_key_sql = 'PRIMARY KEY (id)'
        check_sql= 'CHECK (timestamp >= \'' + str(part_start) + '\' AND timestamp < \'' + str(part_end) + '\')'
        constraint_sql = str(primary_key_sql) + ', ' + str(check_sql)

        table_name = self.get_current_partition_table_name()

        sql = 'CREATE TABLE IF NOT EXISTS ' + table_name + \
              '(' + constraint_sql + ')' + \
              ' INHERITS (\"' + Measurement.__tablename__ + '\");'

        result_proxy = self.engine.execute(sql)
        result_proxy.close()

    def get_current_partition_table_name(self):
        part_start, _ = self.get_partition_boundary_timestamps()
        part_start_string = self.replace_with_underscores(str(part_start))
        return Measurement.__tablename__ + '_' + part_start_string


    def get_partition_boundary_timestamps(self, partition_no=0):
        '''
        partition_no indicates which partition to get timestamps for.
        0 is partition of current time, 1 is next (future) partition,
        -1 is the previous and so on.
        '''
        partition_length_days = 1
        partition_length_seconds = int(dt.timedelta(days=partition_length_days).total_seconds())

        seconds_since_epoch = int(time.time())
        partition_count_since_epoch = int(seconds_since_epoch/partition_length_seconds)

        partition_start_seconds = partition_count_since_epoch * partition_length_seconds
        partition_start_datetime = dt.datetime.utcfromtimestamp(partition_start_seconds)

        tzinfo = pytz.timezone(str(tzlocal.get_localzone()))
        partition_start_datetime = tzinfo.localize(partition_start_datetime)

        #shift back/forth in time according to partition_no parameter
        partition_start_datetime = partition_start_datetime + partition_no * dt.timedelta(seconds=partition_length_seconds)
        partition_end_datetime = partition_start_datetime + dt.timedelta(seconds=partition_length_seconds)

        return partition_start_datetime, partition_end_datetime


    def replace_with_underscores(self, string):
        return string.replace(' ', '_').replace('-', '_').replace(':', '_').replace('+', '_')


class Device(DeclarativeBase):
    __tablename__ = 'Device'

    id = Column(Integer, primary_key=True)
    external_id = Column(String, nullable=False)
    attribute = Column(String, nullable=False)
    unit = Column(String, nullable=False)
    unit_prefix = Column(String)
    sub_type = Column(String, nullable=False)

    __mapper_args__ = {
        'polymorphic_on':sub_type,
        'polymorphic_identity':'Device'
    }

    unique_constraint = UniqueConstraint(external_id, attribute)

    def __repr__(self):
       return "<Device(id='%s', external_id='%s', attribute='%s', unit='%s', unit_prefix='%s', sub_type='%s')>" \
              % (self.id, self.external_id, self.attribute, self.unit, self.unit_prefix, self.sub_type)


class Controller(Device):
    __tablename__ = 'Controller'




    id = Column(Integer, ForeignKey('Device.id'), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity':'Controller'
    }


class Sensor(Device):
    __tablename__ = 'Sensor'

    id = Column(Integer, ForeignKey('Device.id'), primary_key=True)
    value_interval = Column(Time)

    __mapper_args__ = {
        'polymorphic_identity':'Sensor'
    }


class ControlAction(DeclarativeBase):
    __tablename__ = 'ControlAction'

    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey('Device.id'), nullable=False)
    value = Column(Integer, nullable=False)
    created = Column(DateTime, nullable=False)
    schedule = Column(DateTime, nullable=False)
    status = Column(postgresql.ENUM('SCHEDULED', 'SUCCESS', 'FAILED', name='StatusEnum'), nullable=False)
    #TODO add column createdby


class Building(DeclarativeBase):
    __tablename__ = 'Building'

    id = Column(Integer, primary_key=True)
    street_address = Column(String, nullable=False)
    zip_code = Column(String(3), nullable=False)
    country = Column(String, nullable=False)
    floor_min = Column(Integer, nullable=False)
    floor_max = Column(Integer, nullable=False)


class FloorSection(DeclarativeBase):
    __tablename__ = 'FloorSection'

    id = Column(Integer, primary_key=True)
    external_id = Column(String)
    building_id = Column(Integer, ForeignKey('Building.id'), nullable=False)
    floor = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    description = Column(String)

    unique_external_id = UniqueConstraint(external_id, floor, building_id)
    unique_name = UniqueConstraint(name, floor, building_id)


class Room(DeclarativeBase):
    __tablename__ = 'Room'

    id = Column(Integer, primary_key=True)
    floor_section_id = Column(Integer, ForeignKey('FloorSection.id'), nullable=False)
    name = Column(String, nullable=False, unique=True)


class DeviceLocation(DeclarativeBase):
    __tablename__ = 'DeviceLocation'

    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey('Device.id'), nullable=False)
    room_id = Column(Integer, ForeignKey('Room.id'), nullable=False)
    from_time = Column(DateTime(timezone=True), nullable=False)
    to_time = Column(DateTime(timezone=True))


class Measurement(DeclarativeBase):
    __tablename__ = 'Measurement'

    id = Column(Integer, primary_key=True)
    sensor_id = Column(Integer, ForeignKey('Sensor.id'), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    value = Column(Float, nullable=False)

    def __repr__(self):
        return "<Measurement(id='%s', sensor_id='%s', timestamp='%s', value='%s')>" \
                % (self.id, self.sensor_id, self.timestamp, self.value)