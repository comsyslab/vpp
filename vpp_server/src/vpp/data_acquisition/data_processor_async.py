import logging
import threading
from abc import ABCMeta, abstractmethod

import time

from vpp.core import domain_object_factory
from vpp.database.db_manager import DBManager


class AbstractAsyncDataProcessor(object):

    __metaclass__ = ABCMeta

    def __init__(self, data_processor_entity):
        self.logger = logging.getLogger(__name__)

        data_adapter_entity = data_processor_entity.data_adapter_entities[0]
        self.data_adapter = domain_object_factory.get_domain_object_from_entity(data_adapter_entity)

        data_interpreter_entity = data_processor_entity.data_interpreter_entities[0]
        self.data_interpreter = domain_object_factory.get_data_interpreter_from_entity(data_interpreter_entity)

    def listen_for_data(self):
        self.thread = threading.Thread(target=self.data_adapter.listen_for_data, args=(self.process_data,), name=__name__)
        self.thread.setDaemon(True)
        self.thread.start()

    def stop_listening(self):
        self.data_adapter.stop_listening()

    def join(self):
        self.logger.debug("Joining data adapter " + str(self.data_adapter) + "...")
        begin = time.time()
        self.thread.join()
        time_spent = time.time() - begin
        self.logger.debug("...joined in "  + str(time_spent))

    @abstractmethod
    def process_data(self, data):
        pass


class GrundfosDataProcessor(AbstractAsyncDataProcessor):

    def process_data(self, data):
        start_time = time.time()
        result = self.data_interpreter.interpret_data(data)
        meas_dicts = result['measurements']
        sensor_dicts = result['sensors']

        self.db_manager = DBManager()
        self.process_measurements(meas_dicts)
        self.process_sensors(sensor_dicts)
        self.db_manager.close()
        time_spent = time.time() - start_time
        self.logger.info("Processed message in " + str(time_spent) + " seconds.")


    def process_measurements(self, meas_dicts):
        for meas in meas_dicts:
            sensor_external_id_ = meas['sensor_external_id']
            timestamp = meas['timestamp']
            value = meas['value']
            try:
                self.db_manager.create_new_measurement(sensor_external_id_, timestamp, value)
            except:
                self.logger.warning("Could not store measurement for (unknown?) sensor with external ID " + str(sensor_external_id_))
        self.logger.debug("Processed " + str(len(meas_dicts)) + " measurements.")


    def process_sensors(self, sensor_dicts_for_db):
        for sensor_dict in sensor_dicts_for_db:
            external_id = sensor_dict['sensor_external_id']
            new_attribute = sensor_dict['attribute']
            new_unit = sensor_dict['unit']

            existing_sensor_entity = self.db_manager.get_sensor_with_external_id(external_id)
            if existing_sensor_entity is None:
                self.db_manager.create_new_sensor(external_id, new_attribute, new_unit)
            else:
                existing_sensor_entity.attribute = new_attribute
                existing_sensor_entity.unit = new_unit
        self.logger.debug("Processed " + str(len(sensor_dicts_for_db)) + " sensors.")