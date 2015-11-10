from sqlalchemy import *
from sqlalchemy.orm import relationship

from vpp.database.table_manager import DeclarativeBase

__author__ = 'ubbe'


class DataProviderEntity(DeclarativeBase):
    __tablename__ = 'DataProvider'

    id = Column(Integer, primary_key=True)
    data_processor_pickled = Column(String, nullable=False)
    interval = Column(Integer, nullable=False)

    #def __repr__(self):
    #    return "<DataProviderEntity ", id, ">"

    #def __str__(self):
    #    return "<DataProviderEntity ", id, ">"




