import logging


import iso8601
import sqlalchemy
import time

from vpp.database.entities.core_entities import Controller, Device
from vpp.database.entities.core_entities import Sensor
from vpp.database.entities.data_acquisition_entities import DataProviderEntity
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

    def clear_data_providers(self):
        self.session.query(DataProviderEntity).delete()

    def create_new_controller(self, id, attribute, unit, unit_prefix=None):
        controller = Controller(id=id, attribute=attribute, unit=unit, unit_prefix=unit_prefix)
        self.persist_entity(controller)
        return controller

    def create_new_sensor(self, id, attribute, unit, unit_prefix=None, value_interval=None):
        sensor = Sensor(id=id, attribute=attribute,
                        unit=unit, unit_prefix=unit_prefix, value_interval=value_interval)
        self.persist_entity(sensor)
        self.session.flush()
        self.logger.info("Created new sensor " + str(sensor.id))
        return sensor

    def get_device(self, id):
        return self.session.query(Device).filter(Device.id==str(id)).first()

    def create_new_measurements(self, meas_dicts):
        time_begin = time.time()

        table_to_meas_dicts = {}

        time_grouping_begin = time.time()
        for meas in meas_dicts:
            sensor_id = meas['sensor_external_id']
            timestamp = meas['timestamp']
            value = meas['value']
            datetime_w_timezone = iso8601.parse_date(timestamp)
            table_name = self.table_manager.get_partition_table_name(datetime_w_timezone)
            if not table_name in table_to_meas_dicts:
                table_to_meas_dicts[table_name] = []
            table_to_meas_dicts[table_name].append({'sensor_id': sensor_id, 'timestamp': timestamp, 'value': value})
        time_grouping_spent = time.time() - time_grouping_begin

        time_sql_begin = time.time()
        for table_name, meas_list in table_to_meas_dicts.iteritems():
            table = self.table_manager.lookup_table(table_name)
            sql = table.insert().values(meas_list)
            self.session.execute(sql)
            self.logger.info("Created "+ str(len(meas_list)) + " measurements in table " + table_name)
        time_sql_spent = time.time() - time_sql_begin


        time_spent = time.time() - time_begin
        self.logger.debug("Created "+ str(len(meas_dicts)) + " measurements in " + str(time_spent) + " seconds. "
                         "Grouping by table " + str(time_grouping_spent) + " seconds, DB interaction " + str(time_sql_spent) + " seconds.")

    def create_new_measurement(self, sensor_id, timestamp, value):
        datetime_w_timezone = iso8601.parse_date(timestamp)
        table_name = self.table_manager.get_partition_table_name(datetime_w_timezone)
        table = self.table_manager.lookup_table(table_name)
        sql = table.insert().values(sensor_id=sensor_id, timestamp=timestamp, value=value)
        self.session.execute(sql)
        self.logger.debug("Created new measurement " + str(value) + " for sensor " + str(sensor_id))

    def get_measurements_for_sensor(self, sensor_id):
        table = self.table_manager.lookup_table(self.table_manager.measurement_base_table_name)
        sql = table.select('sensor_id=' + str(sensor_id))
        return self.session.execute(sql)

    def get_controller(self, controller_id):
        return self.session.query(Controller).filter_by(id=controller_id).all()

    def get_data_providers(self):
        return self.session.query(DataProviderEntity).all()

    def persist_entity(self, object):
        self.session.add(object)

    def commit(self):
        self.session.commit()

    def close(self):
        self.commit()
        self.session.close()

    def __del__(self):
        self.close()