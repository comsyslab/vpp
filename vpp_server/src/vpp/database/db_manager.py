import logging
from ConfigParser import ConfigParser

import iso8601
import sqlalchemy
import time

from psycopg2._psycopg import IntegrityError

from vpp.config.config_ini_parser import ConfigIniParser
from vpp.database.entities.core_entities import Controller, Device, PredictionEndpoint
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

    def store_new_data_bulk(self, data_dicts):
        time_begin = time.time()

        table_to_data_map = {}

        time_grouping_begin = time.time()
        for data_dict in data_dicts:
            table = self._get_table_for_data_dict(data_dict)

            if not table.name in table_to_data_map:
                table_to_data_map[table.name] = []
            table_to_data_map[table.name].append(data_dict)
        time_grouping_spent_secs = time.time() - time_grouping_begin
        time_grouping_spent_ms = secs_to_ms(time_grouping_spent_secs)

        time_sql_begin = time.time()
        for table_name, mapped_data_dicts in table_to_data_map.iteritems():
            table = self.schema_manager.lookup_table(table_name)
            sql = table.insert().values(mapped_data_dicts)
            try:
                self.session.execute(sql)
                self.logger.info("Created " + str(len(mapped_data_dicts)) + " measurements in table " + table_name)
            except Exception as e:
                self.logger.exception(e)

        time_sql_spent_secs = time.time() - time_sql_begin
        time_sql_spent_ms = secs_to_ms(time_sql_spent_secs)

        time_spent_secs = time.time() - time_begin
        time_spent_ms = secs_to_ms(time_spent_secs)
        self.logger.debug("DBManager processed " + str(len(data_dicts)) + " measurements in " + str(time_spent_ms) + " ms. " +
                          "Grouping by table " + str(time_grouping_spent_ms) + " ms, DB interaction " + str(time_sql_spent_ms) + " ms.")

    def _get_table_for_data_dict(self, data_dict):
        if 'time_received' in data_dict:
            return self.get_prediction_table(data_dict)
        else:
            return self.get_measurement_table(data_dict)

    def get_prediction_table(self, data_dict):
        timestamp = data_dict['time_received']
        datetime_w_timezone = iso8601.parse_date(timestamp)
        return self.schema_manager.get_or_create_prediction_subtable(datetime_w_timezone)

    def get_measurement_table(self, data_dict):
        timestamp = data_dict['timestamp']
        datetime_w_timezone = iso8601.parse_date(timestamp)
        return self.schema_manager.get_or_create_measurement_subtable(datetime_w_timezone)

    def store_new_measurement(self, sensor_id, timestamp, value):
        datetime_w_timezone = iso8601.parse_date(timestamp)
        table = self.schema_manager.get_or_create_measurement_subtable(datetime_w_timezone)
        sql = table.insert().values(sensor_id=sensor_id, timestamp=timestamp, value=value)
        try:
            self.session.execute(sql)
            self.logger.debug("Stored new measurement " + str(value) + " for sensor " + str(sensor_id))
        except Exception as e:
            self.logger.exception(e)

    def get_measurements_for_sensor(self, sensor_id):
        table = self.schema_manager.lookup_table(self.schema_manager.measurement_base_table_name)
        sql = table.select('sensor_id=\'' + str(sensor_id) + '\'')
        return self.session.execute(sql)

    def get_controller(self, controller_id):
        return self.session.query(Controller).filter_by(id=controller_id).all()

    def store_new_prediction_endpoint(self, id, attribute, unit, description=None):
        endpoint = PredictionEndpoint(id=id, attribute=attribute, unit=unit, description=description)
        self.persist_entity(endpoint)
        return endpoint

    def get_prediction_endpoint(self, id):
        return self.session.query(PredictionEndpoint).filter_by(id=id).first()

    def delete_prediction_endpoint(self, id):
        self._pred_endpoint_query(id).delete()

    def _pred_endpoint_query(self, id):
        return self.session.query(PredictionEndpoint).filter(PredictionEndpoint.id == str(id))

    def store_new_prediction(self, pred_endpoint_id, timestamp, value, time_received, value_interval=None):
        '''
        :param pred_endpoint_id: string
        :param timestamp: datetime.datetime
        :param value: string
        :param time_received: datetime.datetime
        :param value_interval: datetime.timedelta
        '''
        timestamp_dt = iso8601.parse_date(timestamp)
        time_received_dt = iso8601.parse_date(time_received)

        interval_sql_value = str(value_interval.days) +  ' days ' + str(value_interval.seconds) + ' seconds'
        #1 year 2 months 3 days 4 hours 5 minutes 6 seconds

        table = self.schema_manager.get_or_create_prediction_subtable(time_received_dt)
        sql = table.insert().values(endpoint_id=pred_endpoint_id, timestamp=timestamp_dt, value=value, time_received=time_received_dt, value_interval=value_interval)
        try:
            self.session.execute(sql)
            self.logger.debug("Stored new prediction " + str(value) + " for endpoint" + str(pred_endpoint_id))
        except Exception as e:
            self.logger.exception(e)

    def persist_entity(self, object):
        self.session.add(object)

    def flush(self):
        self.session.flush()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def close(self):
        self.commit()
        self.session.close()

    def __del__(self):
        self.close()