import logging
from ConfigParser import ConfigParser

import iso8601
import sqlalchemy
import time

from psycopg2._psycopg import IntegrityError

from vpp.config.config_ini_parser import ConfigIniParser
from vpp.database.entities.core_entities import Controller, Device
from vpp.database.entities.core_entities import Sensor

from vpp.database.schema_manager import SchemaManager
from vpp.util import util
from vpp.util.util import secs_to_ms

__author__ = 'ubbe'


class DBManager(object):

    def __init__(self, db_string=None, autoflush=True):
        self.logger = logging.getLogger(__name__)

        if not db_string:
            db_string = ConfigIniParser().get_db_string()

        self.engine = sqlalchemy.create_engine(db_string, echo=False)

        self.schema_manager = SchemaManager(self.engine)

        self.SessionCls = sqlalchemy.orm.sessionmaker(bind=self.engine)
        self.session = self.SessionCls()
        self.session.autoflush = autoflush


    def drop_tables(self):
        self.schema_manager.recreate_schema()

    def create_missing_tables(self):
        self.schema_manager.create_missing_tables()

    def create_new_controller(self, id, attribute, unit, unit_prefix=None):
        controller = Controller(id=id, attribute=attribute, unit=unit, unit_prefix=unit_prefix)
        self.persist_entity(controller)
        return controller

    def create_new_sensor(self, id, attribute, unit, unit_prefix=None, value_interval=None):
        sensor = Sensor(id=id, attribute=attribute,
                        unit=unit, unit_prefix=unit_prefix, value_interval=value_interval)
        self.persist_entity(sensor)
        self.logger.info("Created new sensor " + str(sensor.id))
        return sensor

    def delete_device(self, id):
        self._device_query(id).delete()

    def get_device(self, id):
        return self._device_query(id).first()

    def _device_query(self, id):
        return self.session.query(Device).filter(Device.id == str(id))

    def create_new_measurements(self, meas_dicts):
        time_begin = time.time()

        table_to_meas_dicts = {}

        time_grouping_begin = time.time()
        for meas in meas_dicts:
            sensor_id = meas['sensor_id']
            timestamp = meas['timestamp']
            value = meas['value']
            datetime_w_timezone = iso8601.parse_date(timestamp)
            table = self.schema_manager.get_or_create_measurement_subtable(datetime_w_timezone)
            table_name = table.name
            if not table_name in table_to_meas_dicts:
                table_to_meas_dicts[table_name] = []
            table_to_meas_dicts[table_name].append({'sensor_id': sensor_id, 'timestamp': timestamp, 'value': value})
        time_grouping_spent_secs = time.time() - time_grouping_begin
        time_grouping_spent_ms = secs_to_ms(time_grouping_spent_secs)

        time_sql_begin = time.time()
        for table_name, meas_list in table_to_meas_dicts.iteritems():
            table = self.schema_manager.lookup_table(table_name)
            sql = table.insert().values(meas_list)
            try:
                self.session.execute(sql)
                self.logger.info("Created " + str(len(meas_list)) + " measurements in table " + table_name)
            except Exception as e:
                self.logger.exception(e)

        time_sql_spent_secs = time.time() - time_sql_begin
        time_sql_spent_ms = secs_to_ms(time_sql_spent_secs)

        time_spent_secs = time.time() - time_begin
        time_spent_ms = secs_to_ms(time_spent_secs)
        self.logger.debug("DBManager processed " + str(len(meas_dicts)) + " measurements in " + str(time_spent_ms) + " ms. " +
                         "Grouping by table " + str(time_grouping_spent_ms) + " ms, DB interaction " + str(time_sql_spent_ms) + " ms.")

    def create_new_measurement(self, sensor_id, timestamp, value):
        datetime_w_timezone = iso8601.parse_date(timestamp)
        table = self.schema_manager.get_or_create_measurement_subtable(datetime_w_timezone)
        sql = table.insert().values(sensor_id=sensor_id, timestamp=timestamp, value=value)
        try:
            self.session.execute(sql)
            self.logger.debug("Created new measurement " + str(value) + " for sensor " + str(sensor_id))
        except Exception as e:
            self.logger.exception(e)

    def get_measurements_for_sensor(self, sensor_id):
        table = self.schema_manager.lookup_table(self.schema_manager.measurement_base_table_name)
        sql = table.select('sensor_id=\'' + str(sensor_id) + '\'')
        return self.session.execute(sql)

    def get_controller(self, controller_id):
        return self.session.query(Controller).filter_by(id=controller_id).all()

    def persist_entity(self, object):
        self.session.add(object)

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def close(self):
        self.commit()
        self.session.close()

    def __del__(self):
        self.close()