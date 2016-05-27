import logging
import time

from vpp.config.data_provider_config import DataProviderConfig
from vpp.core import domain_object_factory
from vpp.util import util


class DataProvider(object):

    def __init__(self, ini_file_name):
        self.logger = logging.getLogger(__name__)
        self.name = ini_file_name

        data_provider_config = DataProviderConfig(ini_file_name)

        self.data_adapter = domain_object_factory.get_data_adapter(self, data_provider_config)

        interpreter_fqn = data_provider_config.interpreter_fqn
        self.data_interpreter = domain_object_factory.instantiate_fqn(interpreter_fqn, data_provider_config)

        processor_fqn = data_provider_config.processor_fqn
        self.data_processor = domain_object_factory.instantiate_fqn(processor_fqn)


    def start(self):
        self.data_adapter.start()

    def stop(self):
        self.data_adapter.stop()

    def join(self):
        self.data_adapter.join()

    def interpret_and_process_data(self, data_strings):
        start_time = time.time()
        interpreted_data = self.data_interpreter.interpret_data(*data_strings)
        self.data_processor.process_data(interpreted_data)
        time_spent = time.time() - start_time
        self.logger.info("Processed message in " + str(util.secs_to_ms(time_spent)) + " ms.")


