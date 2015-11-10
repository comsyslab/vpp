import logging

import sqlalchemy

from vpp.database.core_entities import Controller
from vpp.database.core_entities import Sensor
from vpp.database.dataprovider_entities import DataProviderEntity
from vpp.database.table_manager import TableManager

__author__ = 'ubbe'

class DBManager(object):


    '''_instance = None

    @staticmethod
    def __new__(cls, *more):
        if not cls._instance:
            cls._instance = super(DBManager, cls).__new__(cls, *more)
        return cls._instance'''

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        local_db_string = "postgresql://ubbe:ubbep4ss@localhost/vpp"
        self.engine = sqlalchemy.create_engine(local_db_string, echo=False)

        self.table_manager = TableManager(self.engine)

        self.create_missing_tables()

        self.SessionCls = sqlalchemy.orm.sessionmaker(bind=self.engine)

        self.session = self.SessionCls()

    def drop_tables(self):
        self.table_manager.drop_tables()

    def create_missing_tables(self):
        self.table_manager.create_missing_tables()

    def create_new_controller(self, external_id, attribute, unit, unit_prefix=None):
        controller = Controller(external_id=external_id, attribute=attribute, unit=unit, unit_prefix=unit_prefix)
        self.persist_entity(controller)
        return controller

    def create_new_sensor(self, external_id, attribute, unit, unit_prefix=None, value_interval=None):
        sensor = Sensor(external_id=external_id, attribute=attribute,
                        unit=unit, unit_prefix=unit_prefix, value_interval=value_interval)
        self.persist_entity(sensor)
        return sensor

    def get_sensor_with_external_id(self, external_id):
        return self.session.query(Sensor).filter_by(external_id=external_id).first()

    def create_new_measurement(self, sensor_id, timestamp, value):
        table_name = self.table_manager.get_partition_table_name(timestamp)
        table = self.table_manager.lookup_table(table_name)
        sql = table.insert().values(sensor_id=sensor_id, timestamp=timestamp, value=value)
        self.session.execute(sql)

    def get_measurements_for_sensor(self, sensor_id):
        table = self.table_manager.lookup_table(self.table_manager.measurement_base_table_name)
        sql = table.select('sensor_id=' + str(sensor_id))
        return self.session.execute(sql)

    def get_controller(self, controller_id):
        return self.session.query(Controller).filter_by(external_id=controller_id).all()

    def get_data_providers(self):
        return self.session.query(DataProviderEntity).all()

    def persist_entity(self, object):
        self.session.add(object)

    def commit(self):
        self.session.commit()

    def close(self):
        self.session.close()

    def __del__(self):
        self.close()