# coding=UTF-8

import unittest

from vpp.data_acquisition.interpreter.energinet_online_interpreter import EnerginetOnlineInterpreter
from vpp_test.data_acquisition.interpreter.data_provider_config_stub import DataProviderConfigStub


class EnerginetOnlineInterpreterTest(unittest.TestCase):

    def test_energinet_online_interpreter(self):
        data = " 1 Centrale kraftværker DK1\n" \
               " 2 Centrale kraftværker DK2\n" \
               " 3 Decentrale kraftværker DK1\n" \
               " 4 Decentrale kraftværker DK2\n" \
               " 5 Vindmøller DK1\n" \
               " 6 Vindmøller DK2\n" \
               " 7 Udveksling Jylland-Norge\n" \
               " 8 Udveksling Jylland-Sverige\n" \
               " 9 Udveksling Jylland-Tyskland\n" \
               "10 Udveksling Sjælland-Sverige\n" \
               "11 Udveksling Sjælland-Tyskland\n" \
               "12 Udveksling Bornholm-Sverige\n" \
               "13 Udveksling Fyn-Sjaelland\n" \
               "14 Temperatur i Malling\n" \
               "15 Vindhastighed i Malling\n" \
               "16 CO2 udledning\n" \
               "17 Havmøller DK\n" \
               "18 Landmøller DK\n" \
               "19 Solceller DK1\n" \
               "20 Solceller DK2\n" \
               "\n" \
               "Dato og tid      ;      1 ;      2 ;      3 ;      4 ;      5 ;      6 ;      7 ;      8 ;      9 ;     10 ;     11 ;     12 ;     13 ;     14 ;     15 ;     16 ;     17 ;     18 ;     19 ;     20 ;\n" \
               "2014-08-19 00:05 ;    441 ;    297 ;    210 ;     63 ;   2541 ;    696 ;   -949 ;   -734 ;    986 ;  -1058 ;    600 ;     -7 ;   -590 ;     11 ;      6 ;    144 ;   1172 ;   2065 ;    123 ;    456 ;"

        interpreter = EnerginetOnlineInterpreter(DataProviderConfigStub("energinet"))

        data_dict = interpreter.interpret_data(data)

        meas_dicts = data_dict['measurements']
        sensor_dicts = data_dict['sensors']

        self.assertEqual(len(sensor_dicts), 20)
        self.assertEqual(len(meas_dicts), 20)

        index = 0
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_1', 'Centrale kraftværker DK1', 'MW', 441)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_2', 'Centrale kraftværker DK2', 'MW', 297)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_3', 'Decentrale kraftværker DK1', 'MW', 210)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_4', 'Decentrale kraftværker DK2', 'MW', 63)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_5', 'Vindmøller DK1', 'MW', 2541)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_6', 'Vindmøller DK2', 'MW', 696)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_7', 'Udveksling Jylland-Norge', 'MW', -949)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_8', 'Udveksling Jylland-Sverige', 'MW', -734)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_9', 'Udveksling Jylland-Tyskland', 'MW', 986)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_10', 'Udveksling Sjælland-Sverige', 'MW', -1058)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_11', 'Udveksling Sjælland-Tyskland', 'MW', 600)

        index+= 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_12', 'Udveksling Bornholm-Sverige', 'MW', -7)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_13', 'Udveksling Fyn-Sjaelland', 'MW', -590)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_14', 'Temperatur i Malling', 'deg_C', 11)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_15', 'Vindhastighed i Malling', 'm/s', 6)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_16', 'CO2 udledning', 'g/kWh', 144)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_17', 'Havmøller DK', 'MW', 1172)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_18', 'Landmøller DK', 'MW', 2065)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_19', 'Solceller DK1', 'MW', 123)

        index += 1
        self._assert_match(sensor_dicts[index], meas_dicts[index], 'energinet_20', 'Solceller DK2', 'MW', 456)

    def _assert_match(self, sensor_dict, meas_dict, sensor_id_expected, attribute_expected, unit_expected, value_expected):
        self.assertEqual(sensor_dict['sensor_id'], sensor_id_expected)
        self.assertEqual(sensor_dict['attribute'], attribute_expected)
        self.assertEqual(sensor_dict['unit'], unit_expected)
        self.assertEqual(meas_dict['value'], str(value_expected))


if __name__ == '__main__':
    unittest.main()

