from sqlalchemy import *
from sqlalchemy.orm import relationship
from database.table_manager import DeclarativeBase

__author__ = 'ubbe'


class DataProviderEntity(DeclarativeBase):
    __tablename__ = 'DataProvider'

    id = Column(Integer, primary_key=True)

    data_interpreter_id = Column(Integer, ForeignKey('DataInterpreter.id'), nullable=False)
    data_interpreter = relationship("DataInterpreterEntity", uselist=False)

    data_adapter_id = Column(Integer, ForeignKey('DataAdapter.id'), nullable=False)
    data_adapter = relationship("DataAdapterEntity", uselist=False)

    interval = Column(Integer)

    #def __repr__(self):
    #    return "<DataProviderEntity ", id, ">"

    #def __str__(self):
    #    return "<DataProviderEntity ", id, ">"


class DataInterpreterEntity(DeclarativeBase):
    __tablename__ = 'DataInterpreter'

    id = Column(Integer, primary_key=True)


class DataAdapterEntity(DeclarativeBase):
    __tablename__ = 'DataAdapter'

    id = Column(Integer, primary_key=True)


