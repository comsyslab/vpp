import logging
import time

from vpp.config.data_provider_ini_parser import DataProviderIniParser
from vpp.core import domain_object_factory
from vpp.util import util


class DataProvider(object):

    def __init__(self, ini_file_name):
        self.logger = logging.getLogger(__name__)
        self.name = ini_file_name

        ini_parser = DataProviderIniParser(ini_file_name)

        self.data_adapter = domain_object_factory.get_data_adapter(self, ini_parser)

        interpreter_fqn = ini_parser.get_interpreter_fqn()
        self.data_interpreter = domain_object_factory.instantiate_fqn(interpreter_fqn, ini_parser=ini_parser)

        processor_fqn = ini_parser.get_processor_fqn()
        self.data_processor = domain_object_factory.instantiate_fqn(processor_fqn)


    def start(self):
        self.data_adapter.start()

    def stop(self):
        self.data_adapter.stop()

    def join(self):
        self.data_adapter.join()

    def interpret_and_process_data(self, data_strings, db_manager=None):
        start_time = time.time()
        interpreted_data = self.data_interpreter.interpret_data(*data_strings)
        self.data_processor.process_data(interpreted_data, db_manager)
        time_spent = time.time() - start_time
        self.logger.info("Processed message in " + str(util.secs_to_ms(time_spent)) + " ms.")


