import unittest

import datetime
from sqlalchemy.exc import IntegrityError

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
        endpoint = db_manager.create_new_prediction_endpoint(endpoint_id, attribute, unit)
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
        endpoint = db_manager.create_new_prediction_endpoint(endpoint_id, attribute, unit)

        try:
            db_manager.commit()
        except Exception as e:
            db_manager.rollback()
            db_manager.close()
            self.fail(e.message)
            return


        timestamp = datetime.datetime.now().isoformat()
        value = 35
        db_manager.create_new_prediction(endpoint_id, timestamp, value, timestamp)

        db_manager.delete_prediction_endpoint(endpoint_id)
        db_manager.close()


    def get_new_db_manager(self):
        db_string = "postgresql://ubbe:ubbep4ss@localhost/vpp"
        return DBManager(db_string=db_string)



