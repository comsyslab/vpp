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
from vpp_test.data_acquisition.interpreter.data_provider_config_stub import DataProviderConfigStub


class EnerginetCO2InterpreterTest(unittest.TestCase):

    def test_energinet_CO2(self):
        data = '20151027\n' \
               'Timeinterval;CO2\n'\
               '00:00-01:00;224\n'\
               '01:00-02:00;233\n'\
               '23:00-24:00;261'

        interpreter = EnerginetCO2Interpreter(DataProviderConfigStub("energinet_CO2"))

        result = interpreter.interpret_data(data)

        endpoints= result['endpoints']
        self.assertEqual(len(endpoints), 1)

        endpoint = endpoints[0]
        endpoint_id = 'energinet_CO2'
        self.assertEqual(endpoint['id'], endpoint_id)
        self.assertEqual(endpoint['attribute'], 'CO2 emission')
        self.assertEqual(endpoint['unit'], 'g/kWh')
        self.assertEqual(endpoint['description'], 'Predicted CO2 emissions per kWh produced in the Danish power grid.')


        predictions = result['predictions']
        self.assertEqual(len(predictions), 3)

        pred = predictions[0]
        '''endpoint_id': endpoint_id,
                          'timestamp': timestamp.isoformat(),
                          'value': value,
                          'time_received': time_received,
                          'value_interval': interval}'''

        self.assertEqual(pred['endpoint_id'], endpoint_id)
        self.assertEqual(pred['timestamp'], '2015-10-27T00:00:00+01:00')
        self.assertEqual(pred['value'], '224')
        #self.assertEqual(pred['time_received'], '')
        self.assertEqual(pred['value_interval'], datetime.timedelta(hours=1))


if __name__ == '__main__':
    unittest.main()

