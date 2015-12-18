import json
import logging

import re

from vpp.data_acquisition.interpreter.abstract_data_interpreter import AbstractDataInterpreter

__author__ = 'ubbe'


class GrundfosDataInterpreter(AbstractDataInterpreter):

    def __init__(self, device_prefix='grundfos'):
        self.logger = logging.getLogger(__name__)
        self.device_prefix = device_prefix

    def _interpret_string(self, data_string):
        measurements = []
        sensors = []
        data_prefix_stripped = data_string[8:]
        if data_string.startswith("GFKRE003"):
            measurements = self._parse_measurements(data_prefix_stripped)
        elif data_string.startswith("GFKSC002"):

            sensors = self._parse_sensors(data_prefix_stripped)
        else:
            self.logger.warning("Could not parse message with unknown prefix: " + data_string[:20])
        return {'measurements': measurements, 'sensors': sensors}

    def _parse_measurements(self, data_string):
        '''Example data_string:
        {
        "version":3,
        "timestamp":"2014-10-08T09:30:32.750Z",
         "reading":[
             {"sensorId":949,"appartmentId":46,"value":0.000000,"timestamp":"2014-10-08T09:30:32.573Z"},
             {"sensorId":1151,"appartmentId":3,"value":0.292496,"timestamp":"2014-10-08T09:30:32.747Z"},
             {"sensorId":1152,"appartmentId":3,"value":1024.000000,"timestamp":"2014-10-08T09:30:32.747Z"}
         ]
        }'''

        result_dicts = []
        try:
            json_dict = json.loads(data_string)
        except ValueError as e:
            logging.getLogger(__name__).exception(e.message)
            return result_dicts

        json_reading = json_dict['reading']

        for meas_dict in json_reading:
            sensor_id = self._get_id(meas_dict['sensorId'])

            result_dicts.append({'sensor_id': sensor_id,
                                 'timestamp' : meas_dict['timestamp'],
                                 'value' : meas_dict['value']})
        return result_dicts


    def _parse_sensors(self, data_string):
        '''Example data_string
        {
        'appartmentCharacteristic': [
            {'No': 1, 'Size': 39.5, 'Floor': 0, 'appartmentId':66},
            {'No': 2, 'Size': 23.2, 'Floor': 0, 'appartmentId':138}],

        'timestamp': datetime.datetime(2014, 10, 9, 22, 0, 30, 350000),
        '_id': ObjectId('54370594e4b067a4467f33cb'),
        'version': 2,

        'sensorCharacteristic': [
            {'calibrationCoeff': '', 'description': '(*Acc. Energy consumption kWh*)', 'calibrationDate': '2012-11-19T21:22:45.300Z', 'externalRef': '', 'sensorId': 1, 'unit': 'kWh', 'calibrationEquation': ''},
            {'calibrationCoeff': '', 'description': '(*Ambient Temperature C*)', 'calibrationDate': '2012-11-19T21:22:45.320Z', 'externalRef': '', 'sensorId': 2, 'unit': 'C', 'calibrationEquation': ''}]
        }'''

        json_dict = json.loads(data_string)

        json_reading = json_dict['sensorCharacteristic']
        result_dicts = []
        for grundfos_sensor_dict in json_reading:

            id = self._get_id(grundfos_sensor_dict['sensorId'])
            attribute = self._strip_description(grundfos_sensor_dict['description'])

            result_dicts.append({'sensor_id':id,
                                 'attribute' : attribute,
                                 'unit_prefix': "",
                                 'unit' : grundfos_sensor_dict['unit']})
        return result_dicts

    def _strip_description(self, description):
        regex = re.compile('\(\*.*\*\)')
        match = regex.match(description)
        if match:
            start_index = 2
            end_index = len(description) - 2
            return description[start_index:end_index]

        return description


    def _get_id(self, grundfos_id):
        return self.device_prefix + '_' + str(grundfos_id)

