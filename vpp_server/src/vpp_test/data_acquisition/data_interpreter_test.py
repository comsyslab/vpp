
import unittest

from vpp.data_acquisition.data_interpreter import GrundfosMeasurementInterpreter

class DataInterpreterTest(unittest.TestCase):

    def test_grundfos_interpreter(self):

        json = '{"version":3, "timestamp":"2014-10-08T09:30:32.750Z",' \
               '"reading":[{"sensorId":1152,"appartmentId":3,"value":1024.0,"timestamp":"2014-10-08T09:30:32.747Z"}]' \
               '}'

        interpreter = GrundfosMeasurementInterpreter()

        meas_dicts = interpreter.parse_sensors(json)

        self.assertEqual(len(meas_dicts), 1)

        self.assertEqual(meas_dicts[0]['sensor_external_id'], 1152)
        self.assertEqual(meas_dicts[0]['timestamp'], "2014-10-08T09:30:32.747Z")
        self.assertEqual(meas_dicts[0]['value'], 1024.0)

if __name__ == '__main__':
    unittest.main()

