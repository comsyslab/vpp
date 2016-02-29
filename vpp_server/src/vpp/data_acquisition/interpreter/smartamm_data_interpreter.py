import json
import logging

import re

from vpp.data_acquisition.interpreter.abstract_data_interpreter import AbstractDataInterpreter

__author__ = 'ubbe'


class SmartAmmDataInterpreter(AbstractDataInterpreter):

    def __init__(self, data_provider_config = None):
        self.logger = logging.getLogger(__name__)
        self.device_prefix = 'smartamm'
        self.unit_dict = self._create_unit_dict()


    def _interpret_string(self, data_string):
        measurements = []
        sensors = []
        result = {'measurements': measurements, 'sensors': sensors}
        '''SAMRE001{
                    "Attributes":{
                        "Status":0,
                        "CurrentTier1SummationDelivered":281474976710655,
                        "CurrentSummationDelivered":1792351,
                        "InstantaneousDemand":185,
                        "CurrentTier2SummationDelivered":281474976710655},
                    "Time":"2015-11-18T07:36:51.588Z",
                    "Device":"0015BC001B0211D5",
                    "Gateway":"0015BC001C00076D",
                    "Endpoint":2,
                    "Cluster":"Metering"}'''
        if not data_string.startswith("SAMRE001"):
            self.logger.info("Discarded message with unknown prefix: " + data_string[:20])
            return result
        data_prefix_stripped = data_string[8:]
        try:
            json_dict = json.loads(data_prefix_stripped)
        except ValueError as e:
            self.logger.exception(e.message)
            return result
        device_mac = json_dict['Device']
        json_attributes = json_dict['Attributes']
        timestamp = json_dict['Time']
        for attribute, value in json_attributes.items():
            sensor_id = self._get_sensor_id(device_mac, attribute)

            # sensor
            sensors.append({'sensor_id': sensor_id,
                            'attribute': attribute,
                            'unit_prefix': "",
                            'unit': self._lookup_unit(attribute)})
            # measurement
            measurements.append({'sensor_id': sensor_id,
                                 'timestamp': timestamp,
                                 'value': value})
        return result

    def _get_sensor_id(self, device_mac, attribute):
        #max_length = 10
        #if len(attribute) > max_length:
        #    attribute = attribute[:max_length]

        return self.device_prefix + '_' + device_mac + '_' + attribute

    def _create_unit_dict(self):
        unit_dict = {}

        unit_watthour = 'Wh'
        unit_dict['CurrentSummationReceived'] = unit_watthour
        unit_dict['CurrentSummationDelivered'] = unit_watthour
        unit_dict['CurrentTier1SummationDelivered'] = unit_watthour
        unit_dict['CurrentTier2SummationDelivered'] = unit_watthour

        unit_watt = 'W'
        unit_dict['InstantaneousDemand'] = unit_watt
        unit_dict['ActivePower'] = unit_watt
        unit_dict['ReactivePower'] = unit_watt
        unit_dict['ActivePowerPhB'] = unit_watt
        unit_dict['ReactivePowerPhB'] = unit_watt
        unit_dict['ActivePowerPhC'] = unit_watt
        unit_dict['ReactivePowerPhC'] = unit_watt

        unit_celcius = 'deg_C'
        unit_dict['MeasuredValue_Temperature'] = unit_celcius

        unit_dict['Status'] = None

        return unit_dict

    def _lookup_unit(self, attribute):
        try:
            return self.unit_dict[attribute]
        except KeyError:
            if attribute.startswith('ZoneStatus[Alarm'):
                return 'open/closed'

            self.logger.warning("Could not get unit for unknown attribute " + attribute + ".")
            return None
