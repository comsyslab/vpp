# coding=UTF-8
import unittest

from vpp.data_acquisition.interpreter.nordpoolspot_interpreter import NordpoolspotInterpreter


class NordpoolspotInterpreterTest(unittest.TestCase):

    def setUp(self):
        self.expected_sensor_id = 'nordpool_elspot_odense'

    def test_nordpoolspot_interpreter(self):

        data_string = 'Odense-priser i Elspot pr time for 2015;;\n\
                       Hourveighted price pr. hour (DKK/MWh);;\n\
                       Source: Nord Pool;;\n\
                       ;;\n\
                       Date;Hour;\n\
                       ;1;2;3A;3B;4;5;6;7;8;9;10;11;12;13;14;15;16;17;18;19;20;21;22;23;24;Kjøredag;\n\
                       29.12.14;194,09;203,24;219,68;;219,16;219,31;216,11;205,92;270,49;279,94;301,96;308,13;334,76;343,10;327,99;326,21;343,47;385,95;432,37;372,48;325,47;244,30;242,44;229,50;226,97;29.12.14;282,21;;;;Odense priser for Elspot i 2015;;;;'

        interpreter = NordpoolspotInterpreter()

        result = interpreter.interpret_data(data_string)
        meas_dicts = result['measurements']
        sensor_dicts = result['sensors']



        self.assertEqual(len(sensor_dicts), 1)

        self.assertEqual(sensor_dicts[0]['sensor_id'], self.expected_sensor_id)
        self.assertEqual(sensor_dicts[0]['attribute'], 'Price per megawatthour')
        self.assertEqual(sensor_dicts[0]['unit_prefix'], '')
        self.assertEqual(sensor_dicts[0]['unit'], 'DKK/MWh')


        self.assertEqual(len(meas_dicts), 24)

        meas_index = 0
        meas_dict = meas_dicts[meas_index]
        self.assert_measurement(meas_dict, '2014-12-29T00:00:00+01:00', 194.09)

        meas_index = 1
        meas_dict = meas_dicts[meas_index]
        self.assert_measurement(meas_dict, '2014-12-29T01:00:00+01:00', 203.24)

        meas_index = 2
        meas_dict = meas_dicts[meas_index]
        self.assert_measurement(meas_dict, '2014-12-29T02:00:00+01:00', 219.68)

        meas_index = 3
        meas_dict = meas_dicts[meas_index]
        self.assert_measurement(meas_dict, '2014-12-29T03:00:00+01:00', 219.16)

        meas_index = 4
        meas_dict = meas_dicts[meas_index]
        self.assert_measurement(meas_dict, '2014-12-29T04:00:00+01:00', 219.31)


    def assert_measurement(self, meas_dict, expected_timestamp, expected_value):
        self.assertEqual(meas_dict['sensor_id'], self.expected_sensor_id)
        self.assertEqual(meas_dict['timestamp'], expected_timestamp)
        self.assertEqual(float(meas_dict['value']), expected_value)

    def _test_nordpoolspot_interpreter_dst_begin(self):

        data_string = 'Odense-priser i Elspot pr time for 2015;;\n\
                       Hourveighted price pr. hour (DKK/MWh);;\n\
                       Source: Nord Pool;;\n\
                       ;;\n\
                       Date;Hour;\n\
                       ;1;2;3A;3B;4;5;6;7;8;9;10;11;12;13;14;15;16;17;18;19;20;21;22;23;24;Kjøredag;\n\
                       29.03.15;73,93;66,69;;;120,08;160,71;162,65;165,71;162,95;168,92;174,23;177,14;177,74;176,62;175,72;174,23;173,78;174,75;177,44;180,87;181,99;182,44;181,40;179,90;176,24;29.03.15;162,88;;;Summertime;;;;;'

        interpreter = NordpoolspotInterpreter()

        result = interpreter.interpret_data(data_string)
        meas_dicts = result['measurements']

        self.assertEqual(len(meas_dicts), 23)

        meas_index = 1
        meas_dict = meas_dicts[meas_index]
        self.assert_measurement(meas_dict, '2015-03-29T01:00:00+01:00', 66.69)

        meas_index = 2
        meas_dict = meas_dicts[meas_index]
        self.assert_measurement(meas_dict, '2015-03-29T03:00:00+02:00', 120.08)

        meas_index = 3
        meas_dict = meas_dicts[meas_index]
        self.assert_measurement(meas_dict, '2015-03-29T04:00:00+02:00', 160.71)



    def test_nordpoolspot_interpreter_dst_end(self):

        data_string = 'Odense-priser i Elspot pr time for 2015;;\n\
                       Hourveighted price pr. hour (DKK/MWh);;\n\
                       Source: Nord Pool;;\n\
                       ;;\n\
                       Date;Hour;\n\
                       ;1;2;3A;3B;4;5;6;7;8;9;10;11;12;13;14;15;16;17;18;19;20;21;22;23;24;Kjøredag;\n\
                       25.10.15;194,09;203,24;219,68;123,45;219,16;219,31;216,11;205,92;270,49;279,94;301,96;308,13;334,76;343,10;327,99;326,21;343,47;385,95;432,37;372,48;325,47;244,30;242,44;229,50;226,97;29.12.14;282,21;;;;Odense priser for Elspot i 2015;;;;'

        interpreter = NordpoolspotInterpreter()

        result = interpreter.interpret_data(data_string)
        meas_dicts = result['measurements']

        self.assertEqual(len(meas_dicts), 25)


        meas_dict = meas_dicts[1]
        self.assert_measurement(meas_dict, '2015-10-25T01:00:00+02:00', 203.24)

        meas_dict = meas_dicts[2]
        self.assert_measurement(meas_dict, '2015-10-25T02:00:00+02:00', 219.68)

        meas_dict = meas_dicts[3]
        self.assert_measurement(meas_dict, '2015-10-25T02:00:00+01:00', 123.45)

        meas_dict = meas_dicts[4]
        self.assert_measurement(meas_dict, '2015-10-25T03:00:00+01:00', 219.16)

        meas_dict = meas_dicts[5]
        self.assert_measurement(meas_dict, '2015-10-25T04:00:00+01:00', 219.31)


if __name__ == '__main__':
    unittest.main()