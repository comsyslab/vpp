import logging
import sys

from vpp.core.configurator import Configurator
from vpp.core.coordinator import Coordinator
from vpp.data_acquisition.data_processor_async import DefaultAsyncDataProcessor
from vpp.data_acquisition.grundfos_data_interpreter import GrundfosDataInterpreter
from vpp.data_acquisition.smartamm_data_interpreter import SmartAmmDataInterpreter
from vpp.database.db_manager import DBManager
from vpp.database.entities.data_acquisition_entities import DataProviderEntity, DataProcessorEntity, RabbitMQAdapterEntity, \
    DataInterpreterEntity

__author__ = 'ubbe'

def test_create_controller():
    db_manager = DBManager()
    controller_id = "mydevice1"
    controller = db_manager.create_new_controller(id=controller_id, attribute="length", unit="m", unit_prefix=None)

    result = db_manager.get_controller(controller_id)
    for cont in result:
        print "Found controller: " + str(cont)
    db_manager.close()


def create_sensor():
    db_manager = DBManager()

    sensor_id = "temp_sensor_1"
    attribute = "temperature"
    unit = "deg_c"
    sensor = db_manager.create_new_sensor(sensor_id, attribute, unit)

    db_manager.close()

def create_measurement():
    db_manager = DBManager()

    measurement = db_manager.create_new_measurement('temp_sensor_1', "2015-11-18T09:30:32.747Z", 1.37)

    measurements = db_manager.get_measurements_for_sensor('temp_sensor_1')
    for m in measurements:
        print "Found measurement: " + str(m.id) + ", " + str(m.timestamp) + ", " + str(m.value)

    db_manager.close()



def run():
    configurator = Configurator()
    configurator.recreate_db_schema()

    #create_sensor()
    #create_measurement()
    configurator.configure_new_rabbitmq_provider(GrundfosDataInterpreter, "grundfos.sensors", host='localhost', exchange='')
    configurator.configure_new_rabbitmq_provider(interpreter_class=SmartAmmDataInterpreter, queue="smartamm.data", host='localhost', exchange='')

    coordinator = Coordinator()



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        stream=sys.stdout,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%y-%m-%d %H:%M:%S')
    run()