import unittest

import datetime
from sqlalchemy.exc import IntegrityError

from vpp.config.config_ini_parser import ConfigIniParser
from vpp.database.db_manager import DBManager


class DBManagerTest(unittest.TestCase):

    def test_create_new_sensor(self):
        db_manager = self.get_new_db_manager()

        sensor_id = "test_sensor_3"
        attribute = "surface temperature"
        unit = "Kelvin"

        db_manager.create_new_sensor(sensor_id, attribute, unit)
        db_manager.commit()
        db_manager.close()

        db_manager = self.get_new_db_manager()
        sensor = db_manager.get_device(sensor_id)
        self.assertIsNotNone(sensor)
        self.assertEqual(sensor.id, sensor_id)
        self.assertEqual(sensor.attribute, attribute)
        self.assertEqual(sensor.unit, unit)

        db_manager.delete_device(sensor_id)
        db_manager.commit()
        db_manager.close()

    def test_create_duplicate_sensor_fails(self):
        db_manager = self.get_new_db_manager()

        sensor_id = "test_sensor_123"
        attribute = "surface temperature"
        unit = "Kelvin"

        db_manager.create_new_sensor(sensor_id, attribute, unit)
        db_manager.commit()
        db_manager.close()

        db_manager = self.get_new_db_manager()
        db_manager.create_new_sensor(sensor_id, attribute, unit)
        exception = None
        try:
            db_manager.commit()
        except IntegrityError as e:
            exception = e
        finally:
            db_manager.rollback()
            db_manager.delete_device(sensor_id)
            db_manager.commit()

        if not exception :
            self.fail("Committing sensor with duplicate ID should fail!")

    def test_create_new_pred_endpoint(self):
        db_manager = self.get_new_db_manager()

        endpoint_id = 'thor_987'
        attribute = 'wind speed'
        unit = 'm/s'
        endpoint = db_manager.store_new_prediction_endpoint(endpoint_id, attribute, unit)
        try:
            db_manager.commit()
        except Exception as e:
            db_manager.rollback()
            db_manager.close()
            self.fail(e.message)
            return

        db_manager.close()

        db_manager = self.get_new_db_manager()
        retrieved_endpoint = db_manager.get_prediction_endpoint(endpoint_id)
        self.assertIsNotNone(retrieved_endpoint)

        self.assertEqual(endpoint_id, retrieved_endpoint.id)
        self.assertEqual(attribute, retrieved_endpoint.attribute)
        self.assertEqual(unit, retrieved_endpoint.unit)

        db_manager.delete_prediction_endpoint(endpoint_id)
        db_manager.close()


    def test_create_new_prediction(self):
        db_manager = self.get_new_db_manager()

        endpoint_id = 'thor_456'
        attribute = 'wind speed'
        unit = 'm/s'
        endpoint = db_manager.store_new_prediction_endpoint(endpoint_id, attribute, unit)

        try:
            db_manager.commit()
        except Exception as e:
            db_manager.rollback()
            db_manager.close()
            self.fail(e.message)
            return


        timestamp = datetime.datetime.now().isoformat()
        value = 35
        value_interval = datetime.timedelta(minutes=30)

        db_manager.store_new_prediction(endpoint_id, timestamp, value, timestamp, value_interval)

        db_manager.delete_prediction_endpoint(endpoint_id)
        db_manager.close()


    def test_create_new_data_bulk_empty(self):
        db_manager = self.get_new_db_manager(autoflush=True)

        sensor_id1 = "smartamm_0015BC00260000BA_InstantaneousDemand"
        attribute = "InstantaneousDemand"
        unit = "W"

        db_manager.delete_device(sensor_id1)
        db_manager.create_new_sensor(sensor_id1, attribute, unit)

        sensor_id2 = 'smartamm_0015BC00260000BA_CurrentSummationDelivered'
        attribute = "CurrentSummationDelivered"
        unit = "Wh"

        db_manager.delete_device(sensor_id2)
        db_manager.create_new_sensor(sensor_id2, attribute, unit)
        db_manager.commit()

        data_dicts = [{'timestamp': u'2016-02-15T15:46:09.740Z', 'sensor_id': u'smartamm_0015BC00260000BA_InstantaneousDemand', 'value': 55},
                      {'timestamp': u'2016-02-15T15:46:09.740Z', 'sensor_id': u'smartamm_0015BC00260000BA_CurrentSummationDelivered', 'value': 1920517}]
        db_manager.store_new_data_bulk(data_dicts)

        try:
            db_manager.commit()

            db_manager.close()
        except Exception as e:
            db_manager.rollback()
            self.fail(e.message)

    def get_new_db_manager(self, autoflush=False):
        return DBManager(autoflush=autoflush)
