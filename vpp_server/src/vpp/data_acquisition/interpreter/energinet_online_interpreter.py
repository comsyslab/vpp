# coding=UTF-8
import logging
from array import array

import datetime
import iso8601
import pytz
import tzlocal

from vpp.data_acquisition.adapter.adapter_date_strategy import DefaultAdapterFileDateStrategy
from vpp.data_acquisition.interpreter.interpreter_date_strategy import DefaultInterpreterDateStrategy
from vpp.data_acquisition.interpreter.abstract_data_interpreter import AbstractDataInterpreter


class EnerginetOnlineInterpreter(AbstractDataInterpreter):

    def __init__(self, data_provider_config):
        super(EnerginetOnlineInterpreter, self).__init__(data_provider_config)
        self.logger = logging.getLogger(__name__)
        self._init_units_map()
        self.date_helper = DefaultInterpreterDateStrategy(data_provider_config.ftp_config)

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

    def _interpret_string(self, data_string):
        lines = data_string.splitlines()
        sensors = self.parse_sensors(lines)
        measurements = self.parse_measurements(lines)
        return {'measurements': measurements, 'sensors': sensors}

    def parse_sensors(self, lines):
        sensors = []

        end_of_sensors_line = self._find_end_of_sensors_line(lines)
        for line_number in range(0, end_of_sensors_line):
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

    def _find_end_of_sensors_line(self, lines):
        end_of_sensors_line = 0
        for line_number in range(0, len(lines)):
            if lines[line_number].strip() == '':
                end_of_sensors_line = line_number
        return end_of_sensors_line

    def parse_measurements(self, lines):
        measurements = []

        heading_line_no = self.find_heading_line_no(lines)

        if heading_line_no < 0:
            return measurements

        heading_line = lines[heading_line_no]
        attribute_ids_raw = heading_line.split(';')[1:]
        attribute_ids = self._drop_empty_end_elem(attribute_ids_raw)
        first_data_line = heading_line_no + 1
        data_lines = lines[first_data_line:]

        for line in data_lines:
            if len(line.strip()) == 0:
                continue
            values = line.split(';')
            timestamp_naive = self.parse_timestamp(str(values[0]))

            if not self.date_helper.should_process_date(timestamp_naive):
                self.logger.debug('Measurements for date ' + timestamp_naive.isoformat() + ' already processed. Skipping.')
                continue

            timestamp_localized = self.localize(timestamp_naive)

            values = values[1:]
            values = self._drop_empty_end_elem(values)

            for index in range(len(values)):
                sensor_id = self.get_sensor_id(attribute_ids[index])
                value = values[index].strip()

                measurement = {'sensor_id': sensor_id,
                               'timestamp': timestamp_localized,
                               'value': value}

                measurements.append(measurement)

            self.date_helper.update_latest_fetch_date(timestamp_naive)

        return measurements

    def _drop_empty_end_elem(self, list):
        if not list[len(list)-1]:
            return list[:len(list)-1]

    def parse_timestamp(self, date_string):

        try:
            date_string_stripped = date_string.strip()
            return datetime.datetime.strptime(date_string_stripped, '%Y-%m-%d %H:%M')
        except AttributeError as e:
            self.logger.exception(e)
        except Exception as e:
            self.logger.error("Unexpected exception parsing timestamp: " + e.message)

    def localize(self, parsed):
        tzinfo_cph = pytz.timezone('Europe/Copenhagen')
        meas_time = tzinfo_cph.localize(parsed)
        return meas_time.isoformat()

    def get_sensor_id(self, attribute_id):
        return self.id_prefix + '_' + str(attribute_id).strip()


    def find_heading_line_no(self, lines):
        for i in range(0, len(lines)):
            if lines[i].startswith('Dato og tid'):
                return i
        self.logger.warning("No measurements in EnerginetOnline file. Could not find heading line starting with 'Dato og tid'")
        return -1


