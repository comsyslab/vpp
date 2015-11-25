import logging
import time

from vpp.database.db_manager import DBManager


class DefaultProcessingStrategy(object):

    def __init__(self, data_interpreter):
        self.logger = logging.getLogger(__name__)
        self.data_interpreter = data_interpreter

    def process_data(self, data, db_manager=None):
        if not db_manager:           # putting the call to DBManager() directly as default parameter above
            db_manager = DBManager() # apparently causes the same instance to be reused (?).

        start_time = time.time()
        result = self.data_interpreter.interpret_data(data)
        meas_dicts = result['measurements']
        sensor_dicts = result['sensors']

        self.db_manager = db_manager
        self.process_sensors(sensor_dicts)
        self.process_measurements(meas_dicts)
        self.db_manager.close()
        time_spent = time.time() - start_time
        self.logger.info("Processed message in " + str(time_spent) + " seconds.")

    def process_sensors(self, sensor_dicts_for_db):
        for sensor_dict in sensor_dicts_for_db:
            id = sensor_dict['sensor_id']
            new_attribute = sensor_dict['attribute']
            new_unit = sensor_dict['unit']

            existing_sensor_entity = self.db_manager.get_device(id)
            if existing_sensor_entity:
                existing_sensor_entity.attribute = new_attribute
                existing_sensor_entity.unit = new_unit
            else:
                self.db_manager.create_new_sensor(id, new_attribute, new_unit)
        self.logger.debug("Processed " + str(len(sensor_dicts_for_db)) + " sensors.")
        self.db_manager.commit()

    def process_measurements(self, meas_dicts):
        self.db_manager.create_new_measurements(meas_dicts)