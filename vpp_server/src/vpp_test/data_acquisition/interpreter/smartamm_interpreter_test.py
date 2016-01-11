# coding=UTF-8

import logging
import sys
import unittest

from vpp.data_acquisition.interpreter.smartamm_data_interpreter import SmartAmmDataInterpreter


class SmartammInterpreterTest(unittest.TestCase):

    def test_smartamm_interpreter(self):

        logging.basicConfig(level=logging.INFO,
                            stream=sys.stdout,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%y-%m-%d %H:%M:%S')


        json = 'SAMRE001{'\
                    '"Attributes":{'\
                        '"InstantaneousDemand":185},'\
                    '"Time":"2015-11-18T07:36:51.588Z",'\
                    '"Device":"0015BC001B0211D5",'\
                    '"Gateway":"0015BC001C00076D",'\
                    '"Endpoint":2,'\
                    '"Cluster":"Metering"}'''


        interpreter = SmartAmmDataInterpreter()

        data_dict = interpreter.interpret_data(json)
        meas_dicts = data_dict['measurements']
        sensor_dicts = data_dict['sensors']

        self.assertEqual(len(sensor_dicts), 1)
        self.assertEqual(sensor_dicts[0]['sensor_id'], 'smartamm_0015BC001B0211D5_InstantaneousDemand')
        self.assertEqual(sensor_dicts[0]['attribute'], 'InstantaneousDemand')
        self.assertEqual(sensor_dicts[0]['unit'], 'W')

        self.assertEqual(len(meas_dicts), 1)
        self.assertEqual(meas_dicts[0]['sensor_id'], 'smartamm_0015BC001B0211D5_InstantaneousDemand')
        self.assertEqual(meas_dicts[0]['timestamp'], "2015-11-18T07:36:51.588Z")
        self.assertEqual(meas_dicts[0]['value'], 185)


if __name__ == '__main__':
    unittest.main()

