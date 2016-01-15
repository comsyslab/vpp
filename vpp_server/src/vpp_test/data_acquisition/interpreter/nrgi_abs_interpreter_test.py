# coding=UTF-8
import unittest

import datetime

from vpp.data_acquisition.interpreter.nrgi_abs_interpreter import NrgiAbsInterpreter
from vpp.data_acquisition.interpreter.thor_interpreter import ThorInterpreter


class NrgiAbsInterpreterTest(unittest.TestCase):

    def test_nrgi_abs_interpreter(self):
        data_string = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n\
                       <absolutevalues req-id="b5a41bfb-fa9b-464f-8009-728068fd131d">\n\
                           <installationNumber>571313115104260611</installationNumber>\n\
                           <meter-readings obis-code="1-1:1.8.0">\n\
                               <meter-reading validated="true">\n\
                                   <date>2014-03-04T00:00:00.000+01:00</date>\n\
                                   <value>814.921</value>\n\
                               </meter-reading>\n\
                           </meter-readings>\n\
                       </absolutevalues>'

        interpreter = NrgiAbsInterpreter()

        result = interpreter.interpret_data(data_string)
        measurements = result['measurements']
        sensors = result['sensors']

        self.assertEqual(len(sensors), 1)
        expected_sensor_id = 'nrgi_571313115104260611_abs'
        self.assertEqual(sensors[0]['sensor_id'], expected_sensor_id)
        self.assertEqual(sensors[0]['attribute'], 'meter-reading')
        self.assertEqual(sensors[0]['unit'], 'kWh')

        self.assertEqual(len(measurements), 1)
        expected_timestamp = '2014-03-04T00:00:00.000+01:00'
        self.assertEqual(measurements[0]['sensor_id'], expected_sensor_id)
        self.assertEqual(float(measurements[0]['value']), 814.921)
        self.assertEqual(measurements[0]['timestamp'], expected_timestamp)



if __name__ == '__main__':
    unittest.main()