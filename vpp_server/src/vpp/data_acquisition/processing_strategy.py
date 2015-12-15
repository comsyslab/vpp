import logging
from abc import ABCMeta, abstractmethod

from sqlalchemy.exc import IntegrityError

from vpp.database.db_manager import DBManager


class AbstractProcessingStrategy(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def process_data(self, data, db_manager=None):
        pass


class DefaultProcessingStrategy(AbstractProcessingStrategy):

    def process_data(self, data, db_manager=None):
        if not db_manager:  # putting the call to DBManager() directly as default parameter above
            db_manager = DBManager(autoflush=False)  # apparently causes the same instance to be reused (?).
        meas_dicts = data['measurements']
        sensor_dicts = data['sensors']
        self.db_manager = db_manager
        self._process_sensors(sensor_dicts)
        self._process_measurements(meas_dicts)
        self.db_manager.close()

    def _process_sensors(self, sensor_dicts_for_db):
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
        try:
            self.db_manager.commit()
            self.logger.info("Processed " + str(len(sensor_dicts_for_db)) + " sensors.")
        except IntegrityError as e:
            self.logger.exception(e)



    def _process_measurements(self, meas_dicts):
        self.db_manager.create_new_measurements(meas_dicts)