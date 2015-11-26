import logging
import unittest

from vpp.data_acquisition.data_provider import ListeningDataProvider
from vpp.data_acquisition.grundfos_data_interpreter import GrundfosDataInterpreter
from vpp.data_acquisition.data_processor import DefaultDataProcessor
from vpp.database.entities.data_acquisition_entities import DataProviderEntity, RabbitMQAdapterEntity

from vpp.util import util


class DefaultDataProcessorTest(unittest.TestCase):

    def test_default_data_processor_grundfos_measurements(self):

        data = 'GFKRE003{"version":3, "timestamp":"2014-10-08T09:30:32.750Z",' \
                       '"reading":[{"sensorId":1152,"appartmentId":3,"value":1024.0,"timestamp":"2014-10-08T09:30:32.747Z"}]' \
                       '}'

        db_manager = DBManagerStub()
        self.assertEqual(db_manager.meas_dicts, None)

        data_processor = self.get_grundfos_data_processor()

        data_processor.process_data(data, db_manager=db_manager)
        self.assertEqual(len(db_manager.meas_dicts), 1)

        sensor_id = db_manager.meas_dicts[0]['sensor_id']
        timestamp = db_manager.meas_dicts[0]['timestamp']
        value = db_manager.meas_dicts[0]['value']

        self.assertEqual(sensor_id, 'grundfos_1152')
        self.assertEqual(timestamp, "2014-10-08T09:30:32.747Z")
        self.assertEqual(value, 1024.0)



    def test_default_data_processor_grundfos_sensors(self):

        data = 'GFKSC002{' \
                '"appartmentCharacteristic": [{"No": 1, "Size": 39.5, "Floor": 0, "appartmentId":66},{"No": 2, "Size": 23.2, "Floor": 0, "appartmentId":138}],' \
                '"timestamp": "2014-10-08T09:30:32.747Z","version": 2,' \
                '"sensorCharacteristic": [' \
                   '{"calibrationCoeff": "", ' \
                    '"description": "(*Ambient Temperature C*)", ' \
                    '"calibrationDate": "2012-11-19T21:22:45.320Z", ' \
                    '"externalRef": "", ' \
                    '"sensorId": 2, ' \
                    '"unit": "C", ' \
                    '"calibrationEquation": ""}]' \
                '}'

        db_manager = DBManagerStub()

        self.assertEqual(db_manager.sensor_id, None)
        self.assertEqual(db_manager.sensor_attribute, None)
        self.assertEqual(db_manager.sensor_unit, None)

        data_processor = self.get_grundfos_data_processor()
        data_processor.process_data(data, db_manager=db_manager)

        self.assertEqual(db_manager.sensor_id, 'grundfos_2')
        self.assertEqual(db_manager.sensor_attribute, "Ambient Temperature C")
        self.assertEqual(db_manager.sensor_unit, "C")

    def get_grundfos_data_processor(self):
        return DefaultDataProcessor(GrundfosDataInterpreter())


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

