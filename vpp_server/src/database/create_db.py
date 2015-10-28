from _bsddb import DB
from bsddb import db
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from database import tables


class DBCreator(object):
    local_db_string = "postgresql://ubbe:ubbep4ss@localhost/vpp"

    def __init__(self):
        self.engine = create_engine(self.local_db_string, echo=True)

    def create_tables(self):
        tables.Base.metadata.create_all(self.engine)



if __name__ == "__main__":
    db_creator = DBCreator()
    db_creator.create_tables()
