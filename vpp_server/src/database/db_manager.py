import logging

import sqlalchemy
from database.orm_tables import Controller, Sensor
from database.table_manager import TableManager

__author__ = 'ubbe'

class DBManager(object):

    def __init__(self):

        local_db_string = "postgresql://ubbe:ubbep4ss@localhost/vpp"
        self.engine = sqlalchemy.create_engine(local_db_string, echo=False)
        #self.set_logging_levels()

        self.table_manager = TableManager(self.engine)
        self.table_manager.drop_tables()
        self.table_manager.create_missing_tables()

        SessionCls = sqlalchemy.orm.sessionmaker(bind=self.engine)

        self.session = SessionCls()

    def set_logging_levels(self):
        logging.basicConfig()
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)

    def create_new_controller(self, external_id, attribute, unit, unit_prefix=None):
        controller = Controller(external_id=external_id, attribute=attribute, unit=unit, unit_prefix=unit_prefix)
        self.persist_object(controller)
        return controller

    def create_new_sensor(self, external_id, attribute, unit, unit_prefix=None, value_interval=None):
        sensor = Sensor(external_id=external_id, attribute=attribute,
                        unit=unit, unit_prefix=unit_prefix, value_interval=value_interval)
        self.persist_object(sensor)
        return sensor

    def create_new_measurement(self, sensor_id, timestamp, value):
        table_name = self.table_manager.get_partition_table_name(timestamp)
        table = self.table_manager.lookup_table(table_name)
        sql = table.insert().values(sensor_id=sensor_id, timestamp=timestamp, value=value)
        self.session.execute(sql)
        self.session.commit()

    def get_measurements_for_sensor(self, sensor_id):
        table = self.table_manager.lookup_table(self.table_manager.measurement_base_table_name)
        sql = table.select('sensor_id=' + str(sensor_id))
        return self.session.execute(sql)

    def search_controller(self, controller_id):
        return self.session.query(Controller).filter_by(external_id=controller_id)

    def persist_object(self, object):
        self.session.add(object)
        self.session.commit()