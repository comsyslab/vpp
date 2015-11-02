from database.db_manager import DBManager

__author__ = 'ubbe'


class Coordinator:

    def __init__(self):
        self.db_manager = DBManager()
        self.test_db_manager()


    def test_db_manager(self):
        controller_id = "mydevice1"
        controller = self.db_manager.create_new_controller(external_id=controller_id, attribute="length", unit="m", unit_prefix=None)

        sensor_external_id = "temp_sensor_1"
        attribute = "temperature"
        unit = "deg_c"
        sensor = self.db_manager.create_new_sensor(sensor_external_id, attribute, unit)

        measurement = self.db_manager.create_new_measurement(sensor.id, "2015-11-02 23:30:00", 1.37)


        measurements = self.db_manager.get_measurements_for_sensor(sensor.id)
        for m in measurements:
            print "Found measurement: " + str(m.id) + ", " + str(m.timestamp) + ", " + str(m.value)


        result = self.db_manager.search_controller(controller_id)
        for cont in result:
            print "Found controller: " + str(cont)




if __name__ == '__main__':
    coordinator = Coordinator()