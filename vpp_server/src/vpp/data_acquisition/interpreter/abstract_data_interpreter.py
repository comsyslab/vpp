import logging
from _ctypes import ArgumentError
from abc import ABCMeta, abstractmethod


class AbstractDataInterpreter(object):

    __metaclass__ = ABCMeta

    def __init__(self, data_provider_config = None):
        self.logger = logging.getLogger(__name__)

        if data_provider_config is None or data_provider_config.id_prefix is None:
            raise ArgumentError("No data provider config provided, or config contained no id_prefix")

        self.id_prefix = data_provider_config.id_prefix


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