import unittest

from vpp.data_acquisition.data_provider import DataProvider
from vpp.data_acquisition.interpreter.grundfos_data_interpreter import GrundfosDataInterpreter
from vpp.data_acquisition.processing_strategy import DefaultProcessingStrategy
from vpp.database.entities.data_acquisition_entities import DataProviderEntity


class DefaultDataProcessorTest(unittest.TestCase):

    def test_default_data_processor_measurements(self):
        sensor_id = 'grundfos_1152'
        timestamp = '2014-10-08T09:30:32.747Z'
        value = 1024.0

        meas = {'sensor_id': sensor_id,
                'timestamp': timestamp,
                'value': value}

        data = {'sensors': [], 'measurements': [meas]}

        db_manager = DBManagerStub()
        self.assertEqual(db_manager.meas_dicts, None)

        data_processor = DefaultProcessingStrategy()
        data_processor.process_data(data, db_manager=db_manager)
        self.assertEqual(len(db_manager.meas_dicts), 1)

        self.assertEqual(db_manager.meas_dicts[0]['sensor_id'], sensor_id)
        self.assertEqual(db_manager.meas_dicts[0]['timestamp'], timestamp)
        self.assertEqual(db_manager.meas_dicts[0]['value'], value)



    def test_default_data_processor_sensors(self):
        attribute = 'InstantaneousDemand'
        sensor_id = 'grundfos_2'
        unit = 'W'

        sensor_dict = {'sensor_id': sensor_id,
                       'attribute': attribute,
                       'unit': unit}

        data = {'sensors': [sensor_dict], 'measurements': []}

        db_manager = DBManagerStub()
        self.assertEqual(db_manager.sensor_id, None)
        self.assertEqual(db_manager.sensor_attribute, None)
        self.assertEqual(db_manager.sensor_unit, None)

        data_processor = DefaultProcessingStrategy()
        data_processor.process_data(data, db_manager=db_manager)

        self.assertEqual(db_manager.sensor_id, sensor_id)
        self.assertEqual(db_manager.sensor_attribute, attribute)
        self.assertEqual(db_manager.sensor_unit, unit)


class DBManagerStub():
    def __init__(self):
        self.sensor_id = None
        self.sensor_attribute = None
        self.sensor_unit = None
        self.meas_dicts = None

    def get_device(self, id):
        return None

    def create_new_sensor(self, id, attribute, unit, unit_prefix=None, value_interval=None):
        self.sensor_id = id
        self.sensor_attribute = attribute
        self.sensor_unit = unit

    def create_new_measurements(self, meas_dicts):
        self.meas_dicts = meas_dicts

    def commit(self):
        pass

    def close(self):
        pass


if __name__ == '__main__':
    unittest.main()

