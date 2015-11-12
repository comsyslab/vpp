import logging
import pickle
import sys
import threading

import time

import datetime

import pytz
import tzlocal

from vpp.core.data_provider_process_manager import DataProviderProcessManager
from vpp.data_acquisition.data_adapter import RabbitMQAdapter
from vpp.data_acquisition.data_interpreter import GrundfosMeasurementInterpreter
from vpp.data_acquisition.data_processor import DefaultMeasProcessor
from vpp.database.db_manager import DBManager
from vpp.database.entities.dataprovider_entities import DataProviderEntity, DataProcessorEntity, RabbitMQAdapterEntity, \
    DataInterpreterEntity

__author__ = 'ubbe'




class TestCoordinator:

    def __init__(self):
        logging.basicConfig(level=logging.DEBUG,
                            stream=sys.stdout,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%y-%m-%d %H:%M:%S')


        self.logger = logging.getLogger(__name__)


        self.recreate_schema()
        self.test_db_manager()
        #self.clear_data_providers()
        #self.add_new_grundfos_provider()
        #self.run_data_provider_manager()

        #self.test_timestamps()


    def test_db_manager(self):
        db_manager = DBManager()
        controller_id = "mydevice1"
        controller = db_manager.create_new_controller(external_id=controller_id, attribute="length", unit="m", unit_prefix=None)

        sensor_external_id = "temp_sensor_1"
        attribute = "temperature"
        unit = "deg_c"
        sensor = db_manager.create_new_sensor(sensor_external_id, attribute, unit)
        db_manager.commit()


        measurement = db_manager.create_new_measurement(sensor.id, "2015-11-11T09:30:32.747Z", 1.37)

        measurements = db_manager.get_measurements_for_sensor(sensor.id)
        for m in measurements:
            print "Found measurement: " + str(m.id) + ", " + str(m.timestamp) + ", " + str(m.value)

        result = db_manager.get_controller(controller_id)
        for cont in result:
            print "Found controller: " + str(cont)
        db_manager.commit()
        db_manager.close()


    def run_data_provider_manager(self):
        db_manager = DBManager()
        self.logger.info("Found " + str(len(db_manager.get_data_providers())) + " data providers in DB")


        self.dataprovider_process_manager = DataProviderProcessManager()
        self.dataprovider_process_manager.start_process()

        threading._sleep(5)

        self.dataprovider_process_manager.stop_process()
        self.logger.info("TestCoordinator exiting")


    def add_new_grundfos_provider(self):

        data_adapter = RabbitMQAdapterEntity(host="", exchange="", queue="")
        data_interpreter = DataInterpreterEntity(domain_type='GrundfosMeasurementInterpreter')
        data_processor_entity = DataProcessorEntity(domain_type='DefaultMeasProcessor', data_adapter_entities=[data_adapter], data_interpreter_entities=[data_interpreter])
        data_provider_entity = DataProviderEntity(interval=2, data_processor_entity=data_processor_entity)

        db_manager = DBManager()
        db_manager.persist_entity(data_provider_entity)
        db_manager.commit()


    def clear_data_providers(self):
        db_manager = DBManager()
        db_manager.clear_data_providers()
        db_manager.commit()
        db_manager.close()

    def recreate_schema(self):
        db_manager = DBManager()
        db_manager.drop_tables()
        db_manager.create_missing_tables()
        db_manager.commit()
        db_manager.close()


    def test_timestamps(self):
        tzinfo = tzlocal.get_localzone()
        now = datetime.datetime.now(tz=tzinfo)
        print now
        time1 = self.convert_datetime_to_epoch(now)
        print time1
        time2 = self.convert_datetime_to_epoch_2(now)
        print time2
        diff = time2-time1
        print diff


    def convert_datetime_to_epoch(self, timestamp):
        format = "%Y-%m-%d %H:%M:%S"
        timestamp_string = str(timestamp)

        if (timestamp_string.find('.') != -1): #strip second decimals
            timestamp_string = timestamp_string[0:timestamp_string.find('.')]

        struct_time = time.strptime(timestamp_string, format)
        return int(time.mktime(struct_time))

    def convert_datetime_to_epoch_2(self, timestamp):
        tzinfo = tzlocal.get_localzone()
        return (timestamp - datetime.datetime(1970,1,1, tzinfo=tzinfo)).total_seconds()

if __name__ == '__main__':
    coordinator = TestCoordinator()