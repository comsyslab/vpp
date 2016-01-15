
from xml.etree import ElementTree

import pytz

from vpp.data_acquisition.interpreter.abstract_data_interpreter import AbstractDataInterpreter


class NrgiAbsInterpreter(AbstractDataInterpreter):

    def __init__(self, data_provider_config=None):
        self.timezone = pytz.timezone('Europe/Copenhagen')

        self.sensor_prefix = 'nrgi'

        self.fetching_config = None
        if data_provider_config:
            self.fetching_config = data_provider_config.ftp_config

    def _interpret_string(self, data_string):

        root = ElementTree.fromstring(data_string)
        install_no = root.find('installationNumber').text

        sensor_id = self.sensor_prefix + '_' + install_no
        sensors = [{'sensor_id': sensor_id,
                    'attribute': 'meter-reading',
                    'unit_prefix': "",
                    'unit': 'kWh'}]

        meter_readings_tag = root.find('meter-readings')
        readings_tags = meter_readings_tag.findall('meter-reading')
        measurements = []
        for reading_tag in readings_tags:
            date_string = reading_tag.find('date').text
            value = reading_tag.find('value').text

            measurements.append({'sensor_id': sensor_id,
                                 'timestamp': date_string,
                                 'value': value})

        return {'sensors': sensors, 'measurements': measurements}