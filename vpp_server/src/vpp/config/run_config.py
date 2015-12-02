

from vpp.core.configurator import Configurator
from vpp.data_acquisition.interpreter.grundfos_data_interpreter import GrundfosDataInterpreter
from vpp.data_acquisition.interpreter.smartamm_data_interpreter import SmartAmmDataInterpreter


def run_configuration():
    configurator = Configurator()
    configurator.configure_new_rabbitmq_provider(GrundfosDataInterpreter, queue="grundfos.sensors", host='localhost', exchange='')


if __name__=='__main__':
    run_configuration()