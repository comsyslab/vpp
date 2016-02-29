# coding=UTF-8
import json
import logging
import sys
import unittest
from array import array

import datetime

from vpp.data_acquisition.interpreter.energinet_co2_interpreter import EnerginetCO2Interpreter
from vpp.data_acquisition.interpreter.energinet_online_interpreter import EnerginetOnlineInterpreter
from vpp.data_acquisition.interpreter.grundfos_data_interpreter import GrundfosDataInterpreter
from vpp.data_acquisition.interpreter.smartamm_data_interpreter import SmartAmmDataInterpreter
from vpp_test.data_acquisition.interpreter.data_provider_config_stub import DataProviderConfigStub


class GrundfosInterpreterTest(unittest.TestCase):

    def test_grundfos_interpreter(self):

        json = 'GFKRE003{"version":3, "timestamp":"2014-10-08T09:30:32.750Z",' \
               '"reading":[{"sensorId":1152,"appartmentId":3,"value":1024.0,"timestamp":"2014-10-08T09:30:32.747Z"}]' \
               '}'

        interpreter = GrundfosDataInterpreter(DataProviderConfigStub())

        meas_dicts = interpreter.interpret_data(json)['measurements']

        self.assertEqual(len(meas_dicts), 1)

        self.assertEqual(meas_dicts[0]['sensor_id'], 'grundfos_1152')
        self.assertEqual(meas_dicts[0]['timestamp'], "2014-10-08T09:30:32.747Z")
        self.assertEqual(meas_dicts[0]['value'], 1024.0)

    def _test_grundfos_interpreter_sensors(self):

        json_string = "GFKSC002" \
               "{" \
                   "'appartmentCharacteristic': [ " \
                       "{'No': 1, 'Size': 39.5, 'Floor': 0, 'appartmentId':66}, " \
                       "{'No': 2, 'Size': 23.2, 'Floor': 0, 'appartmentId':138}" \
                    "]," \
                    \
                    "'timestamp': '2014-10-08T09:30:32.750Z'"\
                    "'_id': '54370594e4b067a4467f33cb',"\
                    "'version': 2,"\
                    \
                    "'sensorCharacteristic': ["\
                        "{'calibrationCoeff': '', 'description': '(*Accumulated Energy consumption kWh*)', 'calibrationDate': '2012-11-19T21:22:45.300Z', 'externalRef': '', 'sensorId': 1, 'unit': 'kWh', 'calibrationEquation': ''},"\
                        "{'calibrationCoeff': '', 'description': '(*Ambient Temperature C*)', 'calibrationDate': '2012-11-19T21:22:45.320Z', 'externalRef': '', 'sensorId': 2, 'unit': 'C', 'calibrationEquation': ''}" \
                    "]"\
                "}"

        json_dict = json.loads(json_string[8:])

        interpreter = GrundfosDataInterpreter(DataProviderConfigStub())

        sensor_dicts = interpreter.interpret_data(json_string)['sensors']

        self.assertEqual(len(sensor_dicts), 2)

        self.assertEqual(sensor_dicts[0]['sensor_id'], 'grundfos_1')
        self.assertEqual(sensor_dicts[0]['attribute'], 'Accumulated Energy consumption kWh')
        self.assertEqual(sensor_dicts[0]['unit'], 'kWh')
        self.assertEqual(sensor_dicts[0]['unit_prefix'], '')

        self.assertEqual(sensor_dicts[1]['sensor_id'], 'grundfos_2')
        self.assertEqual(sensor_dicts[1]['attribute'], 'Ambient Temperature C')
        self.assertEqual(sensor_dicts[1]['unit'], 'C')
        self.assertEqual(sensor_dicts[1]['unit_prefix'], '')


if __name__ == '__main__':
    unittest.main()

