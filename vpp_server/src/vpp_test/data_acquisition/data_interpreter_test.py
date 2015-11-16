
import unittest

from vpp.data_acquisition.grundfos_data_interpreter import GrundfosDataInterpreter


class DataInterpreterTest(unittest.TestCase):

    def test_grundfos_interpreter(self):

        json = 'GFKRE003{"version":3, "timestamp":"2014-10-08T09:30:32.750Z",' \
               '"reading":[{"sensorId":1152,"appartmentId":3,"value":1024.0,"timestamp":"2014-10-08T09:30:32.747Z"}]' \
               '}'


        interpreter = GrundfosDataInterpreter(None)

        meas_dicts = interpreter.interpret_data(json)['measurements']

        self.assertEqual(len(meas_dicts), 1)

        self.assertEqual(meas_dicts[0]['sensor_id'], 'grundfos_1152')
        self.assertEqual(meas_dicts[0]['timestamp'], "2014-10-08T09:30:32.747Z")
        self.assertEqual(meas_dicts[0]['value'], 1024.0)

if __name__ == '__main__':
    unittest.main()

