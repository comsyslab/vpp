import logging

from vpp.core import domain_object_factory
from vpp.database.db_manager import DBManager


class DefaultMeasProcessor(object):

    def __init__(self, data_processor_entity):

        data_adapter_entity = data_processor_entity.data_adapter_entities[0]
        self.data_adapter = domain_object_factory.get_domain_object_from_entity(data_adapter_entity)

        data_interpreter_entity = data_processor_entity.data_interpreter_entities[0]
        self.data_interpreter = domain_object_factory.get_domain_object_from_entity(data_interpreter_entity)


    def fetch_and_process_data(self, db_manager=DBManager()):
        data = self.data_adapter.fetch_data()

        meas_dicts_for_db = self.data_interpreter.interpret_data(data)

        for meas in meas_dicts_for_db:
            sensor_external_id_ = meas['sensor_external_id']
            timestamp = meas['timestamp']
            value = meas['value']
            db_manager.create_new_measurement(sensor_external_id_, timestamp, value)
            logging.getLogger(__name__).debug("Created new measurement from sensor %d. Timestamp %s, value %d.", sensor_external_id_, timestamp, value)

        db_manager.commit()
        db_manager.close()


class DefaultSensorInfoProcessor():
    def __init__(self, data_processor_entity):
        pass
        #self.data_adapter = data_adapter
        #self.data_interpreter = data_interpreter


    def fetch_and_process_data(self, db_manager=DBManager()):
        data = self.data_adapter.fetch_data()

        sensor_dicts_for_db = self.data_interpreter.interpret_data(data)

        for sensor_dict in sensor_dicts_for_db:
            external_id = sensor_dict['sensor_external_id']
            new_attribute = sensor_dict['attribute']
            new_unit = sensor_dict['unit']

            existing_sensor_entity = db_manager.get_sensor_with_external_id(external_id)
            if existing_sensor_entity is None:
                db_manager.create_new_sensor(external_id, new_attribute, new_unit)
            else:
                existing_sensor_entity.attribute = new_attribute
                existing_sensor_entity.unit = new_unit

        db_manager.commit()
        db_manager.close()