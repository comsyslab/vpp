from database.db_manager import DBManager

__author__ = 'ubbe'


class Coordinator:

    def __init__(self):
        self.db_manager = DBManager()
        self.test_db_manager()


    def test_db_manager(self):
        controller_id1 = "mydevice1"
        controller = self.db_manager.create_new_controller(external_id=controller_id1, attribute="length", unit="m", unit_prefix=None)

        sensor_external_id = "temp_sensor_1"
        attribute = "temperature"
        unit = "deg_c"
        sensor = self.db_manager.create_new_sensor(sensor_external_id, attribute, unit)

        measurement = self.db_manager.create_new_measurement(sensor.id, "2015-10-30 23:30:00+01:00", 1.37)


        for meas in self.db_manager.get_measurements_for_sensor(sensor.id):
            print "Measurement: " + str(meas)

        #print str(controller)
        #print "Looked up " + str(self.db_manager.search_controller(controller_id))




if __name__ == '__main__':
    coordinator = Coordinator()