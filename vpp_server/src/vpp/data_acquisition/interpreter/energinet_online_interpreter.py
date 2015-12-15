# coding=UTF-8
import logging
from array import array

import datetime
import iso8601
import pytz
import tzlocal

from vpp.data_acquisition.interpreter.abstract_data_interpreter import AbstractDataInterpreter


class EnerginetOnlineInterpreter(AbstractDataInterpreter):

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._init_units_map()

    def _init_units_map(self):
        units = [''] * 21
        for i in range(1, 14):
            units[i] = 'MW'
        units[14] = 'deg_C'
        units[15] = 'm/s'
        units[16] = 'g/kWh'
        for i in range(17, 21):
            units[i] = 'MW'

        self.units = units

    def interpret_data(self, data_string):

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
               "\n"\
               "Dato og tid      ;      1 ;      2 ;      3 ;      4 ;      5 ;      6 ;      7 ;      8 ;      9 ;     10 ;     11 ;     12 ;     13 ;     14 ;     15 ;     16 ;     17 ;     18 ;     19 ;     20 ;" \
               "2014-08-19 00:05 ;    441 ;    297 ;    210 ;     63 ;   2541 ;    696 ;   -949 ;   -734 ;    986 ;  -1058 ;    600 ;     -7 ;   -590 ;     11 ;      6 ;    144 ;   1172 ;   2065 ;    123 ;    456 ;"

        lines = data_string.splitlines()
        sensors = self.parse_sensors(lines)
        measurements = self.parse_measurements(lines)

        return {'measurements': measurements, 'sensors': sensors}

    def parse_sensors(self, lines):
        sensors = []
        for line_number in range(0, 20):
            expected_attribute_id = line_number + 1
            line = lines[line_number]
            parsed_attribute_id = int(line[:2])
            if parsed_attribute_id != expected_attribute_id:
                self.logger.error("Line '" + line + "' started with " + str(parsed_attribute_id) + " instead of " + str(expected_attribute_id) + " as expected.")

            sensor_id = self.get_sensor_id(parsed_attribute_id)
            attribute = line[3:]
            unit = self.units[parsed_attribute_id]

            sensor = {'sensor_id': sensor_id,
                      'attribute': attribute,
                      'unit_prefix': "",
                      'unit': unit}

            sensors.append(sensor)
        return sensors

    def parse_measurements(self, lines):
        measurements = []

        heading_line_no = self.find_heading_line_no(lines)

        if heading_line_no < 0:
            return measurements

        heading_line = lines[heading_line_no]
        attribute_ids_raw = heading_line.split(';')[1:]
        attribute_ids = self.drop_empty_end_elem(attribute_ids_raw)
        first_data_line = heading_line_no + 1
        data_lines = lines[first_data_line:]

        for line in data_lines:
            if len(line.strip()) == 0:
                continue
            values = line.split(';')
            timestamp = self.parse_timestamp(str(values[0]))
            values = values[1:]
            values = self.drop_empty_end_elem(values)
            for index in range(len(values)):
                sensor_id = self.get_sensor_id(attribute_ids[index])
                value = values[index].strip()

                measurement = {'sensor_id': sensor_id,
                               'timestamp': timestamp,
                               'value': value}

                measurements.append(measurement)

        return measurements

    def drop_empty_end_elem(self, list):
        if not list[len(list)-1]:
            return list[:len(list)-1]

    def parse_timestamp(self, date_string):
        tzinfo_cph = pytz.timezone('Europe/Copenhagen')
        parsed = datetime.datetime.strptime(date_string.strip(), '%Y-%m-%d %H:%M')
        meas_time = tzinfo_cph.localize(parsed)
        return meas_time.isoformat()

    def get_sensor_id(self, attribute_id):
        return 'energinet_' + str(attribute_id).strip()


    def find_heading_line_no(self, lines):
        for i in range(0, len(lines)):
            if lines[i].startswith('Dato og tid'):
                return i
        self.logger.info("No measurements in EnerginetOnline file. Could not find heading line starting with 'Dato og tid'")
        return -1
