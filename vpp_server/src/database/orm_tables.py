from sqlalchemy import Integer, Column, String, ForeignKey, UniqueConstraint, DateTime, Time
from sqlalchemy.dialects import postgresql
from database.table_manager import DeclarativeBase

__author__ = 'ubbe'


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