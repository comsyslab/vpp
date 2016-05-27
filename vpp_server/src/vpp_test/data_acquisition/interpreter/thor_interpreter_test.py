# coding=UTF-8
import unittest

import datetime

from vpp.data_acquisition.interpreter.thor_interpreter import ThorInterpreter
from vpp_test.data_acquisition.interpreter.data_provider_config_stub import DataProviderConfigStub


class ThorInterpreterTest(unittest.TestCase):

    def test_thor_interpreter(self):
        data_string = '[BOF]\n\
                       [BOH]\n\
                       ASCIICode.Separator=44\n\
                       ASCIICode.Decimal=46\n\
                       MissingValues="-999","-999.9","-99.99"\n\
                       Resolution=Hour.01\n\
                       IntegrationPeriod=C\n\
                       DateFormat=YYYYMMDD\n\
                       TimeFormat=HH:NN\n\
                       Station.Name="Aarhus (DK)"\n\
                       Station.Parameters.Count=6\n\
                       Station.Parameters(1).Name="Total precipitation"\n\
                       Station.Parameters(1).Unit="mm/hour"\n\
                       Station.Parameters(1).Pos=1\n\
                       Station.Parameters(2).Name="Convective precipitation"\n\
                       Station.Parameters(2).Unit="mm/hour"\n\
                       Station.Parameters(2).Pos=2\n\
                       Station.Parameters(3).Name="Snow precipitation"\n\
                       Station.Parameters(3).Unit="mm/hour"\n\
                       Station.Parameters(3).Pos=3\n\
                       Station.Parameters(4).Name="MSL pressure"\n\
                       Station.Parameters(4).Unit="hPa"\n\
                       Station.Parameters(4).Pos=4\n\
                       Station.Parameters(5).Name="2m Temperature"\n\
                       Station.Parameters(5).Unit="K"\n\
                       Station.Parameters(5).Pos=5\n\
                       Station.Parameters(6).Name="80m Temperature"\n\
                       Station.Parameters(6).Unit="K"\n\
                       Station.Parameters(6).Pos=6\n\
                       [EOH]\n\
                       [BOD]\n\
                       20140412, 03:00,  0.0,  0.1,  0.2, 1018, -99.9,   5.1\n\
                       [EOD]\n\
                       [EOF]'

        interpreter = ThorInterpreter(DataProviderConfigStub("thor"))

        result = interpreter.interpret_data(data_string)
        pred_dicts = result['predictions']
        endpoint_dicts = result['endpoints']


        self.assertEqual(len(endpoint_dicts), 6)

        endpoint_1 = 'thor_aarhus_(dk)_01'
        endpoint_2 = 'thor_aarhus_(dk)_02'
        endpoint_3 = 'thor_aarhus_(dk)_03'
        endpoint_4 = 'thor_aarhus_(dk)_04'
        endpoint_5 = 'thor_aarhus_(dk)_05'
        endpoint_6 = 'thor_aarhus_(dk)_06'

        self.assert_endpoint(endpoint_dicts[0], endpoint_1, 'Total precipitation', 'mm/hour')
        self.assert_endpoint(endpoint_dicts[1], endpoint_2, 'Convective precipitation', 'mm/hour')
        self.assert_endpoint(endpoint_dicts[2], endpoint_3, 'Snow precipitation', 'mm/hour')
        self.assert_endpoint(endpoint_dicts[3], endpoint_4, 'MSL pressure', 'hPa')
        self.assert_endpoint(endpoint_dicts[4], endpoint_5, '2m Temperature', 'K')
        self.assert_endpoint(endpoint_dicts[5], endpoint_6, '80m Temperature', 'K')

        self.assertEqual(len(pred_dicts), 6)

        timestamp = '2014-04-12T03:00:00+02:00'
        interval = 3600
        self.assert_prediction(pred_dicts[0], endpoint_1, 0.0, timestamp, interval)
        self.assert_prediction(pred_dicts[1], endpoint_2, 0.1, timestamp, interval)
        self.assert_prediction(pred_dicts[2], endpoint_3, 0.2, timestamp, interval)
        self.assert_prediction(pred_dicts[3], endpoint_4, 1018, timestamp, interval)
        self.assert_prediction(pred_dicts[4], endpoint_5, -99.9, timestamp, interval)
        self.assert_prediction(pred_dicts[5], endpoint_6, 5.1, timestamp, interval)

    def assert_endpoint(self, endpoint_dict, expected_id, expected_attribute, expected_unit):
        self.assertEqual(endpoint_dict['id'], expected_id)
        self.assertEqual(endpoint_dict['attribute'], expected_attribute)
        self.assertEqual(endpoint_dict['unit'], expected_unit)

    def assert_prediction(self, pred_dict, expected_endpoint_id, expected_value, expected_timestamp, expected_value_interval):
        self.assertEqual(pred_dict['endpoint_id'], expected_endpoint_id)
        self.assertEqual(float(pred_dict['value']), expected_value)
        self.assertEqual(pred_dict['timestamp'], expected_timestamp)
        self.assertEqual(pred_dict['value_interval'].seconds, expected_value_interval)





if __name__ == '__main__':
    unittest.main()