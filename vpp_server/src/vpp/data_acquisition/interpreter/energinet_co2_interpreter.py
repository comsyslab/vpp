import datetime

import pytz
import tzlocal

from vpp.data_acquisition.interpreter.abstract_data_interpreter import AbstractDataInterpreter


class EnerginetCO2Interpreter(AbstractDataInterpreter):

    def _interpret_string(self, data_string):
        endpoint_id = 'energinet_CO2'
        endpoint = {'id': endpoint_id,
                    'attribute': 'CO2 emission',
                    'unit': 'g/kWh',
                    'description': 'Predicted CO2 emissions per kWh produced in the Danish power grid.'}
        timezone = pytz.timezone('Europe/Copenhagen')
        lines = data_string.splitlines()
        if len(lines) == 0:
            return

        try:
            first_line = lines[0]
            date = datetime.datetime.strptime(first_line, '%Y%m%d')
        except AttributeError as e:
            self.logger.error(e.message)
        data_lines = lines[2:]
        time_received = datetime.datetime.now(tzlocal.get_localzone())
        predictions = []
        for line in data_lines:
            if len(line.strip()) == 0:
                continue
            values = line.split(';')

            interval_start_hour, interval = self._parse_interval_string(values[0])
            timestamp_naive = date + interval_start_hour
            timestamp = timezone.localize(timestamp_naive)

            value = values[1]

            prediction = {'endpoint_id': endpoint_id,
                          'timestamp': timestamp.isoformat(),
                          'value': value,
                          'time_received': time_received.isoformat(),
                          'value_interval': interval}

            predictions.append(prediction)
        return {'predictions': predictions, 'endpoints': [endpoint]}

    def _parse_interval_string(self, interval_string):
        times = interval_string.split('-')

        start_hour, start_min = self.parse_hour_min_string(times[0])
        end_hour, end_min = self.parse_hour_min_string(times[1])

        start_delta = datetime.timedelta(hours=start_hour, minutes=start_min)
        end_delta = datetime.timedelta(hours=end_hour, minutes=end_min)

        duration = end_delta - start_delta

        return start_delta, duration

    def parse_hour_min_string(self, hour_min_string):
        hour_and_min = hour_min_string.split(':')
        return int(hour_and_min[0]), int(hour_and_min[1])


