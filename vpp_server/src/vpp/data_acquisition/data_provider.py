import logging
import time

from vpp.core import domain_object_factory


class DataProvider(object):

    def __init__(self, entity):
        self.logger = logging.getLogger(__name__)
        self.entity = entity
        self.data_adapter = domain_object_factory.get_data_adapter_from_entity(entity.data_adapter_entity, self)

        self.data_interpreter = domain_object_factory.instantiate_fqn(entity.data_interpreter_domain_type)
        self.processing_strategy = domain_object_factory.instantiate_fqn(entity.processing_strategy_domain_type)


    def get_id(self):
        return self.entity.id

    def start(self):
        self.data_adapter.start()

    def stop(self):
        self.data_adapter.stop()

    def join(self):
        self.data_adapter.join()

    def interpret_and_process_data(self, data_string, db_manager=None):
        start_time = time.time()
        interpreted_data = self.data_interpreter.interpret_data(data_string)
        self.processing_strategy.process_data(interpreted_data, db_manager)
        time_spent = time.time() - start_time
        self.logger.info("Processed message in " + str(time_spent) + " seconds.")


