# coding=UTF-8

import logging
import sys
import unittest
from array import array

import datetime

from vpp.data_acquisition.interpreter.energinet_co2_interpreter import EnerginetCO2Interpreter
from vpp.data_acquisition.interpreter.energinet_online_interpreter import EnerginetOnlineInterpreter
from vpp.data_acquisition.interpreter.grundfos_data_interpreter import GrundfosDataInterpreter
from vpp.data_acquisition.interpreter.smartamm_data_interpreter import SmartAmmDataInterpreter


class GrundfosInterpreterTest(unittest.TestCase):

    def test_grundfos_interpreter(self):

        json = 'GFKRE003{"version":3, "timestamp":"2014-10-08T09:30:32.750Z",' \
               '"reading":[{"sensorId":1152,"appartmentId":3,"value":1024.0,"timestamp":"2014-10-08T09:30:32.747Z"}]' \
               '}'

        interpreter = GrundfosDataInterpreter()

        meas_dicts = interpreter.interpret_data(json)['measurements']

        self.assertEqual(len(meas_dicts), 1)

        self.assertEqual(meas_dicts[0]['sensor_id'], 'grundfos_1152')
        self.assertEqual(meas_dicts[0]['timestamp'], "2014-10-08T09:30:32.747Z")
        self.assertEqual(meas_dicts[0]['value'], 1024.0)

if __name__ == '__main__':
    unittest.main()

