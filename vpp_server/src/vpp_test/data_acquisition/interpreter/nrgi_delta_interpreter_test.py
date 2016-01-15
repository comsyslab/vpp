# coding=UTF-8
import unittest

import datetime

from vpp.data_acquisition.interpreter.nrgi_abs_interpreter import NrgiAbsInterpreter
from vpp.data_acquisition.interpreter.nrgi_delta_interpreter import NrgiDeltaInterpreter
from vpp.data_acquisition.interpreter.thor_interpreter import ThorInterpreter


class NrgiAbsInterpreterTest(unittest.TestCase):

    def test_nergi_abs_interpreter(self):
        data_string = '<deltavalues req-id="0bd5f90a-b28a-45bc-8112-634c1bd3de03">\n\
                         <installationNumber>571313115104261182</installationNumber>\n\
                         <meter-readings interval-length="15min" obis-code="1-1:1.9.0">\n\
                            <meter-reading validated="true">\n\
                                <date>2014-03-04T00:00:00.000+01:00</date>\n\
                                <value>0.02</value>\n\
                            </meter-reading>\n\
                            <meter-reading validated="true">\n\
                                <date>2014-03-04T00:15:00.000+01:00</date>\n\
                                <value>0.01</value>\n\
                            </meter-reading>\n\
                         </meter-readings>\n\
                       </deltavalues>'

        interpreter = NrgiDeltaInterpreter()

        result = interpreter.interpret_data(data_string)
        measurements = result['measurements']
        sensors = result['sensors']

        self.assertEqual(len(sensors), 1)
        self.assertEqual(sensors[0]['sensor_id'], 'nrgi_571313115104261182_delta')
        self.assertEqual(sensors[0]['attribute'], 'meter-delta')
        self.assertEqual(sensors[0]['unit'], 'kWh')

        self.assertEqual(len(measurements), 2)

        self.assertEqual(measurements[0]['sensor_id'], 'nrgi_571313115104261182_delta')
        self.assertEqual(float(measurements[0]['value']), 0.02)
        self.assertEqual(measurements[0]['timestamp'], '2014-03-04T00:00:00.000+01:00')

        self.assertEqual(measurements[1]['sensor_id'], 'nrgi_571313115104261182_delta')
        self.assertEqual(float(measurements[1]['value']), 0.01)
        self.assertEqual(measurements[1]['timestamp'], '2014-03-04T00:15:00.000+01:00')



if __name__ == '__main__':
    unittest.main()