import logging
from abc import ABCMeta, abstractmethod


class AbstractDataInterpreter(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def interpret_data(self, *data_strings):
        measurements = []
        sensors = []
        endpoints = []
        predictions = []

        for data_string in data_strings:
            temp_result = self._interpret_string(data_string)

            if 'measurements' in temp_result:
                measurements.extend(temp_result['measurements'])

            if 'sensors' in temp_result:
                sensors.extend(temp_result['sensors'])

            if 'endpoints' in temp_result:
                endpoints.extend(temp_result['endpoints'])

            if 'predictions' in temp_result:
                predictions.extend(temp_result['predictions'])

        return {'measurements': measurements,
                'sensors': sensors,
                'endpoints': endpoints,
                'predictions': predictions}

    @abstractmethod
    def _interpret_string(self, data_string):
        pass