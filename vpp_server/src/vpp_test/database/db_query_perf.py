import time

from vpp.database.db_manager import DBManager


class DBQueryPerf():
    def __init__(self, queryObject):
        self.query = queryObject

    def measurements_for_all_sensors(self):
        self.db_manager = DBManager()

        sensors = self.db_manager.get_sensors().fetchall()
        index = 0
        processed_count = 0
        combined_time = 0
        combined_meas = 0.0

        for sensor in sensors:
            if index % 100 != 0:
                index += 1
                continue

            start_time = time.time()
            result = self.query.doQuery(self.db_manager, sensor.id)
            end_time = time.time()
            time_spent = end_time - start_time
            processed_count += 1
            combined_time += time_spent
            combined_meas += len(result)

            if index % 500 == 0:
                print "Scanned {} sensors".format(index)
            index += 1

        self.db_manager.close()

        avg_time = combined_time / processed_count
        avg_meas = combined_meas / processed_count

        return str(processed_count) + ' sensors, avg ' + str(avg_meas) + ' measurements, avg fetch time ' + str(avg_time) + ' secs'

class SimpleMeasQuery:
    def doQuery(self, db_manager, sensor_id):
        return db_manager.get_measurements_for_sensor(sensor_id).fetchall()

class ComplexMeasQuery:
    def doQuery(self, db_manager, sensor_id):
        #interval_start = '2016-02-01 11:55:00.0+00'
        interval_start = '2016-04-03 14:00:00+02'
        #interval_end = '2016-02-01 11:56:00.0+00'
        interval_end = '2016-04-04 14:00:00+02'
        return db_manager.get_measurements_for_sensor_in_interval(sensor_id, interval_start, interval_end).fetchall()


if __name__ == '__main__':

    result = DBQueryPerf(SimpleMeasQuery()).measurements_for_all_sensors()
    print "All measurements: " + result

    result = DBQueryPerf(ComplexMeasQuery()).measurements_for_all_sensors()
    print "Measurements in interval: " + result