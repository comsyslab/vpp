from sqlalchemy import *
from sqlalchemy.orm import relationship

from vpp.database.table_manager import DeclarativeBase

__author__ = 'ubbe'


class DataProviderEntity(DeclarativeBase):
    __tablename__ = 'DataProvider'

    id = Column(Integer, primary_key=True)
    domain_type = Column(String, nullable=False)
    interval = Column(Integer)

    data_processor_id = Column(Integer, ForeignKey('DataProcessor.id'), nullable=False)
    data_processor_entity = relationship('DataProcessorEntity', uselist=False)

    data_adapter_id = Column(Integer, ForeignKey('DataAdapter.id'), nullable=False)
    data_adapter_entity = relationship('DataAdapterEntity', uselist=False)


class DataProcessorEntity(DeclarativeBase):
    __tablename__ = 'DataProcessor'

    id = Column(Integer, primary_key=True)
    domain_type = Column(String, nullable=False)
    data_interpreter_domain_type = Column(String, nullable=False)


class DataAdapterEntity(DeclarativeBase):
    __tablename__ = 'DataAdapter'

    id = Column(Integer, primary_key=True)
    sub_type = Column(String, nullable=False)

    __mapper_args__ = {
        'polymorphic_on':sub_type,
        'polymorphic_identity':'DataAdapter'
    }


class RabbitMQAdapterEntity(DataAdapterEntity):
    __tablename__ = 'RabbitMQAdapter'
    id = Column(Integer, ForeignKey('DataAdapter.id'), primary_key=True)
    host = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    queue = Column(String, nullable=False)

    __mapper_args__ = {
        'polymorphic_identity':'RabbitMQAdapter'
    }

class FTPAdapterEntity(DataAdapterEntity):
    __tablename__ = 'FTPAdapter'
    id = Column(Integer, ForeignKey('DataAdapter.id'), primary_key=True)
    host = Column(String, nullable=False)
    user = Column(String, nullable=False)
    password = Column(String, nullable=False)
    file = Column(String, nullable=False)

    __mapper_args__ = {
        'polymorphic_identity':'FTPAdapter'
    }