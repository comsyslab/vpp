import json
import logging

__author__ = 'ubbe'


class GrundfosMeasurementInterpreter(object):



    '''{"version":3,
        "timestamp":"2014-10-08T09:30:32.750Z",
         "reading":[
             {"sensorId":949,"appartmentId":46,"value":0.000000,"timestamp":"2014-10-08T09:30:32.573Z"},
             {"sensorId":1151,"appartmentId":3,"value":0.292496,"timestamp":"2014-10-08T09:30:32.747Z"},
             {"sensorId":1152,"appartmentId":3,"value":1024.000000,"timestamp":"2014-10-08T09:30:32.747Z"}
         ]
       }'''

    def __init__(self, entity):
        self.entity = entity

    def interpret_data(self, data_string=""):
        result_dicts = []

        try:
            json_dict = json.loads(data_string)
        except ValueError as e:
            logging.getLogger(__name__).exception(e.message)
            return result_dicts

        json_reading = json_dict['reading']

        for meas_dict in json_reading:
            result_dicts.append({'sensor_external_id':meas_dict['sensorId'],
                                 'timestamp' : meas_dict['timestamp'],
                                 'value' : meas_dict['value']})

        return result_dicts


class GrundfosSensorInfoInterpreter(object):

    '''{'appartmentCharacteristic': [
            {'No': 1, 'Size': 39.5, 'Floor': 0, 'appartmentId':66},
            {'No': 2, 'Size': 23.2, 'Floor': 0, 'appartmentId':138}],

        'timestamp': datetime.datetime(2014, 10, 9, 22, 0, 30, 350000),
        '_id': ObjectId('54370594e4b067a4467f33cb'),
        'version': 2,

        'sensorCharacteristic': [
            {'calibrationCoeff': '', 'description': '(*Acc. Energy consumption kWh*)', 'calibrationDate': '2012-11-19T21:22:45.300Z', 'externalRef': '', 'sensorId': 1, 'unit': 'kWh', 'calibrationEquation': ''},
            {'calibrationCoeff': '', 'description': '(*Ambient Temperature C*)', 'calibrationDate': '2012-11-19T21:22:45.320Z', 'externalRef': '', 'sensorId': 2, 'unit': 'C', 'calibrationEquation': ''}]
        }'''

    def interpret_data(self, data_string):
        json_dict = json.loads(data_string)

        json_reading = json_dict['sensorCharacteristic']
        result_dicts = []
        for sensor_dict in json_reading:
            result_dicts.append({'sensor_external_id':sensor_dict['sensorId'],
                                 'attribute' : sensor_dict['description'],
                                 'unit_prefix': "",
                                 'unit' : sensor_dict['unit']})

        return result_dicts