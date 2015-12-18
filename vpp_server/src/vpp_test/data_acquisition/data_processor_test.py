import unittest

from vpp.data_acquisition.processing_strategy import DefaultMeasurementProcessingStrategy, \
    DefaultPredictionProcessingStrategy


class DefaultDataProcessorTest(unittest.TestCase):

    def test_default_data_processor_measurements(self):
        sensor_id = 'grundfos_1152'
        timestamp = '2014-10-08T09:30:32.747Z'
        value = 1024.0

        meas = {'sensor_id': sensor_id,
                'timestamp': timestamp,
                'value': value}

        data = {'sensors': [], 'measurements': [meas]}

        db_manager = DBManagerStub()
        self.assertEqual(db_manager.meas_dicts, None)

        data_processor = DefaultMeasurementProcessingStrategy()
        data_processor.process_data(data, db_manager=db_manager)
        self.assertEqual(len(db_manager.meas_dicts), 1)

        self.assertEqual(db_manager.meas_dicts[0]['sensor_id'], sensor_id)
        self.assertEqual(db_manager.meas_dicts[0]['timestamp'], timestamp)
        self.assertEqual(db_manager.meas_dicts[0]['value'], value)



    def test_default_data_processor_sensors(self):
        attribute = 'InstantaneousDemand'
        sensor_id = 'grundfos_2'
        unit = 'W'

        sensor_dict = {'sensor_id': sensor_id,
                       'attribute': attribute,
                       'unit': unit}

        data = {'sensors': [sensor_dict], 'measurements': []}

        db_manager = DBManagerStub()
        self.assertEqual(db_manager.id, None)
        self.assertEqual(db_manager.attribute, None)
        self.assertEqual(db_manager.unit, None)

        data_processor = DefaultMeasurementProcessingStrategy()
        data_processor.process_data(data, db_manager=db_manager)

        self.assertEqual(db_manager.id, sensor_id)
        self.assertEqual(db_manager.attribute, attribute)
        self.assertEqual(db_manager.unit, unit)


    def test_default_prediction_processor_endpoints(self):
        attribute = 'Wind speed'
        endpoint_id = 'thor_23'
        unit = 'm/s'
        description = 'prediction for top of the Sky Mountain'

        endpoint_dict = {'id': endpoint_id,
                         'attribute': attribute,
                         'unit': unit,
                         'description': description}

        data = {'endpoints': [endpoint_dict], 'predictions': []}

        db_manager = DBManagerStub()
        self.assertEqual(db_manager.id, None)
        self.assertEqual(db_manager.attribute, None)
        self.assertEqual(db_manager.unit, None)
        self.assertEqual(db_manager.desc, None)

        data_processor = DefaultPredictionProcessingStrategy()
        data_processor.process_data(data, db_manager=db_manager)

        self.assertEqual(db_manager.id, endpoint_id)
        self.assertEqual(db_manager.attribute, attribute)
        self.assertEqual(db_manager.unit, unit)
        self.assertEqual(db_manager.desc, description)

    def test_default_prediction_processor_predictions(self):
        endpoint_id = 'test_id_123'
        timestamp = '2014-10-08T09:30:32.747Z'
        value = 1024.0
        value_interval = 5

        pred = {'endpoint_id': endpoint_id,
                'timestamp': timestamp,
                'value': value,
                'time_received': timestamp,
                'value_interval': value_interval}

        data = {'endpoints': [], 'predictions': [pred]}

        db_manager = DBManagerStub()
        self.assertEqual(db_manager.id, None)
        self.assertEqual(db_manager.timestamp, None)
        self.assertEqual(db_manager.value, None)
        self.assertEqual(db_manager.value_interval, None)
        self.assertEqual(db_manager.time_received, None)


        data_processor = DefaultPredictionProcessingStrategy()
        data_processor.process_data(data, db_manager=db_manager)

        self.assertEqual(db_manager.id, endpoint_id)
        self.assertEqual(db_manager.timestamp, timestamp)
        self.assertEqual(db_manager.value, value)
        self.assertEqual(db_manager.value_interval, value_interval)
        self.assertEqual(db_manager.time_received, timestamp)


class DBManagerStub():
    def __init__(self):
        self.id = None
        self.attribute = None
        self.unit = None
        self.meas_dicts = None
        self.desc = None
        self.timestamp = None
        self.value = None
        self.time_received = None
        self.value_interval = None

    def get_device(self, id):
        return None

    def create_new_sensor(self, id, attribute, unit, unit_prefix=None, value_interval=None):
        self.id = id
        self.attribute = attribute
        self.unit = unit

    def create_new_measurements(self, meas_dicts):
        self.meas_dicts = meas_dicts

    def get_prediction_endpoint(self, id):
        return None

    def create_new_prediction_endpoint(self, id, attribute, unit, description=None):
        self.id = id
        self.attribute = attribute
        self.unit = unit
        self.desc = description

    def create_new_prediction(self, endpoint_id, timestamp, value, time_received, value_interval=None):
        self.id = endpoint_id
        self.timestamp = timestamp
        self.value = value
        self.time_received = time_received
        self.value_interval = value_interval

    def commit(self):
        pass

    def close(self):
        pass


if __name__ == '__main__':
    unittest.main()

