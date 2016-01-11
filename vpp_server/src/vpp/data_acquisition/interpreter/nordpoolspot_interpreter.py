import logging

import datetime

import pytz
import tzlocal

from vpp.data_acquisition.interpreter.abstract_data_interpreter import AbstractDataInterpreter


class NordpoolspotInterpreter(AbstractDataInterpreter):

    def __init__(self, ):
        self.logger = logging.getLogger(__name__)
        self.timezone = pytz.timezone('Europe/Copenhagen')
        self.sensor_id = 'nordpool_elspot_odense'

    def _interpret_string(self, data_string):
        data_w_dots = data_string.replace(',', '.')
        data_lines = data_w_dots.splitlines()

        first_meas_line = self._find_heading_line_no(data_lines) + 2

        measurement_lines = data_lines[first_meas_line:]

        measurements = self.parse_measurements(measurement_lines)


        sensors = [{'sensor_id':self.sensor_id,
                    'attribute' : 'Price per megawatthour',
                    'unit_prefix': "",
                    'unit' : 'DKK/MWh'}]


        return {'sensors': sensors, 'measurements': measurements}

    def parse_measurements(self, measurement_lines):
        measurements = []
        for line in measurement_lines:
            if len(line.strip()) == 0:
                continue
            measurements.extend(self.parse_line(line))

        return measurements

    def parse_line(self, line):
        values = line.split(';')
        date = self.get_date(values)
        measurements = []
        for i in range(1, 26):
            value = values[i].strip()
            if value == '':
                continue

            hour_offset = 1
            if i > 3:
                hour_offset = 2

            hour = i - hour_offset

            timestamp_naive = datetime.datetime(date.year, date.month, date.day, hour)

            if i == 3 and values[i + 1].strip(): # Check if this is just before leaving DST
                timestamp_w_tz = self.localize_timestamp_just_before_dst_end(timestamp_naive)
            else:
                timestamp_w_tz = self.timezone.localize(timestamp_naive)

            measurement = {'sensor_id': self.sensor_id,
                           'timestamp': timestamp_w_tz.isoformat(),
                           'value': value}

            measurements.append(measurement)

        return measurements

    def localize_timestamp_just_before_dst_end(self, timestamp_naive):
        # Workaround to tell timezone.localize(...) that this timestamp is still in the DST timezone,
        # since there is no way for the library to know whether 02:00 is before or after the clock adjustment.
        one_minute = datetime.timedelta(minutes=1)
        timestamp_naive = timestamp_naive - one_minute
        timestamp_w_tz = self.timezone.localize(timestamp_naive)
        timestamp_w_tz = timestamp_w_tz + one_minute
        return timestamp_w_tz

    def _find_heading_line_no(self, lines):
        for i in range(0, len(lines)):
            expected_heading_line = 'Date;Hour;'
            line = lines[i].strip()
            if line.startswith(expected_heading_line):
                return i
        self.logger.error("Could not find heading line starting with '" + expected_heading_line + "'")
        return -1

    def get_date(self, values):
        date_string = values[0].strip()
        day = int(date_string[0:2])
        month = int(date_string[3:5])
        year = int(date_string[6:]) + 2000
        return datetime.date(year, month, day)