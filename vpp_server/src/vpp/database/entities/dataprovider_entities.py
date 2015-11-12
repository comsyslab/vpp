from sqlalchemy import *
from sqlalchemy.orm import relationship

from vpp.database.table_manager import DeclarativeBase

__author__ = 'ubbe'


class DataProviderEntity(DeclarativeBase):
    __tablename__ = 'DataProvider'

    id = Column(Integer, primary_key=True)
    interval = Column(Integer, nullable=False)

    data_processor_id = Column(Integer, ForeignKey('DataProcessor.id'), nullable=False)
    data_processor_entity = relationship('DataProcessorEntity')

    #def __repr__(self):
    #    return "<DataProviderEntity ", id, ">"

    #def __str__(self):
    #    return "<DataProviderEntity ", id, ">"

class DataProcessorEntity(DeclarativeBase):
    __tablename__ = 'DataProcessor'

    id = Column(Integer, primary_key=True)
    domain_type = Column(String, nullable=False)
    data_adapter_entities = relationship('DataAdapterEntity')
    data_interpreter_entities = relationship('DataInterpreterEntity')



class DataInterpreterEntity(DeclarativeBase):
    __tablename__ = 'DataInterpreter'

    id = Column(Integer, primary_key=True)
    domain_type = Column(String, nullable=False)
    data_processor = Column(Integer, ForeignKey('DataProcessor.id'), nullable=False)



class DataAdapterEntity(DeclarativeBase):
    __tablename__ = 'DataAdapter'

    id = Column(Integer, primary_key=True)
    data_processor = Column(Integer, ForeignKey('DataProcessor.id'), nullable=False)
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