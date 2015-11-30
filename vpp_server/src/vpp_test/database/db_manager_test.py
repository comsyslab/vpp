import unittest

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

    def get_new_db_manager(self):
        db_string = "postgresql://ubbe:ubbep4ss@localhost/vpp"
        return DBManager(db_string=db_string)



