from vpp.core.configurator import Configurator
from vpp.data_acquisition.grundfos_data_interpreter import GrundfosDataInterpreter
from vpp.data_acquisition.smartamm_data_interpreter import SmartAmmDataInterpreter


def run_configuration():
    configurator = Configurator()
    configurator.recreate_db_schema()
    configurator.configure_new_rabbitmq_provider(GrundfosDataInterpreter, "grundfos.sensors", host='localhost', exchange='')
    configurator.configure_new_rabbitmq_provider(interpreter_class=SmartAmmDataInterpreter, queue="smartamm.data", host='localhost', exchange='')

if __name__=='__main__':
    run_configuration()