import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative.api import has_inherited_table
from sqlalchemy.sql.schema import UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Time, Float, DateTime, Enum, MetaData, Table

__author__ = 'ubbe@eng.au.dk'

Base = declarative_base()

class Device(Base):
     __tablename__ = 'Device'

     id = Column(Integer, primary_key=True)
     external_id = Column(String, nullable=False)
     attribute = Column(String, nullable=False)
     unit = Column(String, nullable=False)
     unit_prefix = Column(String, nullable=False)
     sub_type = Column(String, nullable=False)

     __mapper_args__ = {
         'polymorphic_on':sub_type,
         'polymorphic_identity':'Device'
     }

     unique_constraint = UniqueConstraint(external_id, attribute)

     def __repr__(self):
        return "<User(name='%s', fullname='%s', password='%s')>" \
               % (self.name, self.fullname, self.password)

class Sensor(Device):
    __tablename__ = 'Sensor'

    id = Column(Integer, ForeignKey('Device.id'), primary_key=True)
    value_interval = Column(Time)


class ControlAction(Base):
    __tablename__ = 'ControlAction'

    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey('Device.id'), nullable=False)
    value = Column(Integer, nullable=False)
    created = Column(DateTime, nullable=False)
    schedule = Column(DateTime, nullable=False)
    status = Column(postgresql.ENUM('SCHEDULED', 'SUCCESS', 'FAILED', name='StatusEnum'), nullable=False)
    #TODO add column createdby


class Building(Base):
    __tablename__ = 'Building'

    id = Column(Integer, primary_key=True)
    street_address = Column(String, nullable=False)
    zip_code = Column(String(3), nullable=False)
    country = Column(String, nullable=False)
    floor_min = Column(Integer, nullable=False)
    floor_max = Column(Integer, nullable=False)

class FloorSection(Base):
    __tablename__ = 'FloorSection'

    id = Column(Integer, primary_key=True)
    external_id = Column(String)
    building_id = Column(Integer, ForeignKey('Building.id'), nullable=False)
    floor = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    description = Column(String)

    unique_external_id = UniqueConstraint(external_id, floor, building_id)
    unique_name = UniqueConstraint(name, floor, building_id)

class Room(Base):
    __tablename__ = 'Room'

    id = Column(Integer, primary_key=True)
    floor_section_id = Column(Integer, ForeignKey('FloorSection.id'), nullable=False)
    name = Column(String, nullable=False, unique=True)

class DeviceLocation(Base):
    __tablename__ = 'DeviceLocation'

    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey('Device.id'), nullable=False)
    room_id = Column(Integer, ForeignKey('Room.id'), nullable=False)
    from_time = Column(DateTime, nullable=False)
    to_time = Column(DateTime)

class Measurement(Base):
    #TODO partition per 24 hours
    __tablename__ = 'Measurement'

    id = Column(Integer, primary_key=True)
    sensor_id = Column(Integer, ForeignKey('Sensor.id'), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    value = Column(Float, nullable=False)


def create_measurement_tables(engine):
    '''Create initial subtable for the present day'''


    result_proxy = engine.execute('SELECT * FROM \"Room\";')
    #result_proxy.

    #print "children: " + meas_table_sub.in


