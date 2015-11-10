
import unittest

from vpp.data_acquisition.data_interpreter import GrundfosMeasurementInterpreter, GrundfosSensorInfoInterpreter
from vpp.data_acquisition.data_processor import DefaultMeasProcessor, DefaultSensorProcessor


class DataProcessorTest(unittest.TestCase):

    def test_default_meas_processor(self):

        class DataAdapterStub():
            def fetch_data(self):
                return '{"version":3, "timestamp":"2014-10-08T09:30:32.750Z",' \
                       '"reading":[{"sensorId":1152,"appartmentId":3,"value":1024.0,"timestamp":"2014-10-08T09:30:32.747Z"}]' \
                       '}'

        class DBManagerStub():
            def __init__(self):
                self.sensor_id = None
                self.timestamp = None
                self.value = None

            def create_new_measurement(self, sensor_id, timestamp, value):
                self.sensor_id = sensor_id
                self.timestamp = timestamp
                self.value = value

            def commit_writes(self):
                pass

            def close(self):
                pass

        db_manager = DBManagerStub()

        self.assertEqual(db_manager.sensor_id, None)
        self.assertEqual(db_manager.timestamp, None)
        self.assertEqual(db_manager.value, None)

        data_processor = DefaultMeasProcessor(DataAdapterStub(),
                                              GrundfosMeasurementInterpreter())
        data_processor.fetch_and_process_data(db_manager=db_manager)

        self.assertEqual(db_manager.sensor_id, 1152)
        self.assertEqual(db_manager.timestamp, "2014-10-08T09:30:32.747Z")
        self.assertEqual(db_manager.value, 1024.0)


    def test_default_sensor_processor(self):

        class DataAdapterStub():
            def fetch_data(self):
                return '{' \
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

        class DBManagerStub():
            def __init__(self):
                self.external_id = None
                self.attribute = None
                self.unit = None

            def get_sensor_with_external_id(self, external_id):
                return None

            def create_new_sensor(self, external_id, attribute, unit, unit_prefix=None, value_interval=None):
                self.external_id = external_id
                self.attribute = attribute
                self.unit = unit

            def commit(self):
                pass

            def close(self):
                pass

        db_manager = DBManagerStub()

        self.assertEqual(db_manager.external_id, None)
        self.assertEqual(db_manager.attribute, None)
        self.assertEqual(db_manager.unit, None)

        data_processor = DefaultSensorProcessor(DataAdapterStub(),
                                                GrundfosSensorInfoInterpreter())
        data_processor.fetch_and_process_data(db_manager=db_manager)

        self.assertEqual(db_manager.external_id, 2)
        self.assertEqual(db_manager.attribute, "(*Ambient Temperature C*)")
        self.assertEqual(db_manager.unit, "C")


if __name__ == '__main__':
    unittest.main()

