
import logging

import time

import datetime
import iso8601
import pytz
import tzlocal

from vpp.data_acquisition.interpreter.abstract_data_interpreter import AbstractDataInterpreter


class ThorInterpreter(AbstractDataInterpreter):

    def __init__(self, data_provider_config=None):
        super(ThorInterpreter, self).__init__(data_provider_config)
        self.logger = logging.getLogger(__name__)
        self.timezone = pytz.timezone('Europe/Copenhagen')
        self.fetching_config = data_provider_config.ftp_config

    def _interpret_string(self, data_string):

        lines = data_string.splitlines()

        station_name_line_no, self.station_name = self._find_station_name_line(lines)

        eoh_line_no = self._find_line(lines, '[EOH]')
        endpoint_lines = lines[station_name_line_no + 2 : eoh_line_no]
        endpoints = self._parse_endpoints(endpoint_lines)

        first_pred_line_no = self._find_line(lines, '[BOD]') + 1
        eod_line_no = self._find_line(lines, '[EOD]')
        pred_lines = lines[first_pred_line_no:eod_line_no]
        predictions = self._parse_predictions(endpoints, pred_lines)

        return {'endpoints': endpoints, 'predictions': predictions}

    def _find_station_name_line(self, lines):
        station_name_string = 'Station.Name'
        line_no = self._find_line(lines, station_name_string)
        if line_no == -1:
            return -1, None

        line = lines[line_no].strip()
        station_name = self._get_value(line)
        station_name = station_name.replace(' ', '_').lower()
        return line_no, station_name

    def _get_value(self, line):
        sep_index = line.find('=')
        value = line[sep_index+1:]
        return value.strip().replace('"', '')


    def _find_line(self, lines, line_prefix):
        for i in range(0, len(lines)):
            line = lines[i].strip()
            if line.startswith(line_prefix):
                return i
        return -1

    def _parse_endpoints(self, lines):
        endpoints = []

        for i in range(0, len(lines), 3):
            attribute = self._get_value(lines[i])
            unit = self._get_value(lines[i+1])
            value = self._get_value(lines[i+2])
            value = value.zfill(2)
            id = self.id_prefix + '_' + self.station_name + '_' + value
            endpoints.append({'id': id, 'attribute': attribute, 'unit': unit, 'description': ''})

        return endpoints

    def _parse_predictions(self, endpoints, lines):
        #20140412, 00:00,  0.0,  0.1,  0.2, 1018, -99.9,   5.1
        predictions = []
        for line in lines:
            values = line.split(',')
            timestamp_parsed = values[0].strip() + ' ' + values[1].strip()

            timestamp_dt = datetime.datetime.strptime(timestamp_parsed, '%Y%m%d %H:%M')
            timestamp = self.timezone.localize(timestamp_dt)
            timestamp = timestamp.isoformat()

            time_received = datetime.datetime.now(self.timezone).isoformat()
            value_interval = datetime.timedelta(hours=1)

            values = values[2:]

            for i in range(0, len(values)):
                value = values[i].strip()
                endpoint_id = endpoints[i]['id']
                predictions.append({'endpoint_id': endpoint_id,
                                    'timestamp': timestamp,
                                    'value': value,
                                    'time_received': time_received,
                                    'value_interval': value_interval})

        return predictions
