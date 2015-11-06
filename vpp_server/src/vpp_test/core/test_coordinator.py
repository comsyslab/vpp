import logging

import sys
import threading

from vpp.core.data_provider_process_manager import DataProviderProcessManager
from vpp.database.dataprovider_entities import DataAdapterEntity, DataInterpreterEntity, DataProviderEntity
from vpp.database.db_manager import DBManager

__author__ = 'ubbe'




class TestCoordinator:

    def __init__(self):
        logging.basicConfig(level=logging.DEBUG,
                            stream=sys.stdout,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%y-%m-%d %H:%M:%S')


        self.logger = logging.getLogger(__name__)

        self.db_manager = DBManager()

        #self.test_db_manager()


        self.test_data_provider_manager()

    def test_db_manager(self):
        controller_id = "mydevice1"
        controller = self.db_manager.create_new_controller(external_id=controller_id, attribute="length", unit="m", unit_prefix=None)

        sensor_external_id = "temp_sensor_1"
        attribute = "temperature"
        unit = "deg_c"
        sensor = self.db_manager.create_new_sensor(sensor_external_id, attribute, unit)

        measurement = self.db_manager.create_new_measurement(sensor.id, "2015-11-06 23:30:00", 1.37)

        measurements = self.db_manager.get_measurements_for_sensor(sensor.id)
        for m in measurements:
            print "Found measurement: " + str(m.id) + ", " + str(m.timestamp) + ", " + str(m.value)

        result = self.db_manager.get_controller(controller_id)
        for cont in result:
            print "Found controller: " + str(cont)


    def test_data_provider_manager(self):

        #self.add_new_data_providers()

        self.logger.info("Found " + str(len(self.db_manager.get_data_providers())) + " data providers in DB")


        self.dataprovider_manager = DataProviderProcessManager()

        self.dataprovider_manager.start_process()

        threading._sleep(3)

        self.dataprovider_manager.stop_process()
        self.logger.info("TestCoordinator exiting")

    def add_new_data_providers(self):
        for _ in range(0, 3):
            data_adapter_entity = DataAdapterEntity()
            data_interpreter_entity = DataInterpreterEntity()

            data_provider_entity = DataProviderEntity(data_adapter=data_adapter_entity,
                                                      data_interpreter=data_interpreter_entity, interval=2)
            self.db_manager.persist_object(data_provider_entity)


if __name__ == '__main__':
    coordinator = TestCoordinator()