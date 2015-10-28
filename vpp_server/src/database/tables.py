from sqlalchemy.sql.schema import UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Time

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

