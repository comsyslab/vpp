import logging
import sys
import threading

import re

from vpp.core.data_provider_process_manager import DataProviderProcessManager
from vpp.database.db_manager import DBManager
from vpp.database.entities.data_acquisition_entities import DataProviderEntity, DataProcessorEntity, RabbitMQAdapterEntity, \
    DataInterpreterEntity

__author__ = 'ubbe'


class TestCoordinator:

    def __init__(self):
        logging.basicConfig(level=logging.INFO,
                            stream=sys.stdout,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%y-%m-%d %H:%M:%S')


        self.logger = logging.getLogger(__name__)


        #self.drop_and_recreate_db_schema()

        #self.clear_data_providers()

        #self.configure_new_grundfos_data_provider()


        #self.create_sensor()
        #self.create_measurement()

        #self.test_create_controller()

        self.run_data_provider_manager()


    def test_create_controller(self):
        db_manager = DBManager()
        controller_id = "mydevice1"
        controller = db_manager.create_new_controller(external_id=controller_id, attribute="length", unit="m", unit_prefix=None)

        result = db_manager.get_controller(controller_id)
        for cont in result:
            print "Found controller: " + str(cont)
        db_manager.close()


    def create_sensor(self):
        db_manager = DBManager()

        sensor_external_id = "temp_sensor_1"
        attribute = "temperature"
        unit = "deg_c"
        sensor = db_manager.create_new_sensor(sensor_external_id, attribute, unit)

        db_manager.close()

    def create_measurement(self):
        db_manager = DBManager()

        measurement = db_manager.create_new_measurement('temp_sensor_1', "2015-11-13T09:30:32.747Z", 1.37)

        measurements = db_manager.get_measurements_for_sensor_external_id('temp_sensor_1')
        for m in measurements:
            print "Found measurement: " + str(m.id) + ", " + str(m.timestamp) + ", " + str(m.value)

        db_manager.close()


    def run_data_provider_manager(self):
        db_manager = DBManager()
        self.logger.info("Found " + str(len(db_manager.get_data_providers())) + " data providers in DB")


        self.dataprovider_process_manager = DataProviderProcessManager()
        self.dataprovider_process_manager.start_process()

        #threading._sleep(30)

        #self.dataprovider_process_manager.stop_process()
        self.logger.info("TestCoordinator exiting")


    def configure_new_grundfos_data_provider(self):
        data_adapter = RabbitMQAdapterEntity(host="localhost", exchange="", queue="grundfos.sensors")

        data_interpreter = DataInterpreterEntity(domain_type='GrundfosDataInterpreter')
        data_processor_entity = DataProcessorEntity(domain_type='GrundfosDataProcessor', data_adapter_entities=[data_adapter], data_interpreter_entities=[data_interpreter])
        data_provider_entity = DataProviderEntity(interval=0, data_processor_entity=data_processor_entity)

        db_manager = DBManager()
        db_manager.persist_entity(data_provider_entity)
        db_manager.close()


    def clear_data_providers(self):
        db_manager = DBManager()
        db_manager.clear_data_providers()
        db_manager.close()

    def drop_and_recreate_db_schema(self):
        db_manager = DBManager()
        db_manager.drop_tables()
        db_manager.create_missing_tables()
        db_manager.close()




if __name__ == '__main__':
    coordinator = TestCoordinator()