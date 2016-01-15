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


class DefaultMeasurementProcessingStrategy(AbstractProcessingStrategy):

    def process_data(self, data, db_manager=None):
        if not db_manager:  # putting the call to DBManager() directly as default parameter above
            db_manager = DBManager(autoflush=False)  # apparently causes the same instance to be reused (?).
        meas_dicts = data['measurements']
        sensor_dicts = data['sensors']
        self.db_manager = db_manager
        self._process_sensors(sensor_dicts)
        self._process_data(meas_dicts)
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
                self.db_manager.flush()
        try:
            self.db_manager.commit()
            self.logger.info("Processed " + str(len(sensor_dicts_for_db)) + " sensors.")
        except IntegrityError as e:
            self.logger.exception(e)

    def _process_data(self, meas_dicts):
        self.db_manager.store_new_data_bulk(meas_dicts)


class DefaultPredictionProcessingStrategy(AbstractProcessingStrategy):

    def process_data(self, data, db_manager=None):
        if not db_manager:  # putting the call to DBManager() directly as default parameter above
            db_manager = DBManager(autoflush=False)  # apparently causes the same instance to be reused (?).
        endpoint_dicts = data['endpoints']
        pred_dicts = data['predictions']
        self.db_manager = db_manager
        self._process_endpoints(endpoint_dicts)
        self._process_data(pred_dicts)
        self.db_manager.close()

    def _process_endpoints(self, endpoint_dicts):
        '''
        :param endpoint_dicts: List of dicts each containing: {'id', 'attribute', 'unit', 'description'}
        '''

        for endpoint_dict in endpoint_dicts:
            id = endpoint_dict['id']
            new_attribute = endpoint_dict['attribute']
            new_unit = endpoint_dict['unit']
            description = endpoint_dict['description']

            existing_endpoint = self.db_manager.get_prediction_endpoint(id)
            if existing_endpoint:
                existing_endpoint.attribute = new_attribute
                existing_endpoint.unit = new_unit
            else:
                self.db_manager.store_new_prediction_endpoint(id, new_attribute, new_unit, description)
        try:
            self.db_manager.commit()
            self.logger.info("Processed " + str(len(endpoint_dicts)) + " endpoints.")

        except IntegrityError as e:
            self.logger.exception(e)

    def _process_data(self, pred_dicts):
        '''
        :param pred_dicts: List of dicts each containing: {'endpoint_id', 'timestamp', 'value', 'time_received', 'value_interval'}
        '''
        self.db_manager.store_new_data_bulk(pred_dicts)
