import logging

from vpp.data_acquisition.data_processor_async import DefaultAsyncDataProcessor
from vpp.database.db_manager import DBManager
from vpp.database.entities.data_acquisition_entities import DataInterpreterEntity, DataProcessorEntity, \
    DataProviderEntity, RabbitMQAdapterEntity


class Configurator(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def recreate_db_schema(self):
        db_manager = DBManager()
        db_manager.drop_tables()
        db_manager.create_missing_tables()
        db_manager.close()

    def clear_data_providers(self):
        db_manager = DBManager()
        db_manager.clear_data_providers()
        db_manager.close()

    def configure_new_rabbitmq_provider(self, interpreter_class, queue, host="localhost", exchange=""):
        data_adapter = RabbitMQAdapterEntity(host=host, exchange=exchange, queue=queue)

        data_interpreter = DataInterpreterEntity(domain_type=self._get_fully_qualified_name(interpreter_class))
        data_processor_entity = DataProcessorEntity(domain_type=self._get_fully_qualified_name(DefaultAsyncDataProcessor), data_adapter_entities=[data_adapter], data_interpreter_entities=[data_interpreter])
        data_provider_entity = DataProviderEntity(interval=0, data_processor_entity=data_processor_entity)

        db_manager = DBManager()
        db_manager.persist_entity(data_provider_entity)
        db_manager.close()

    def _get_fully_qualified_name(self, cls):
        return cls.__module__ + "." + cls.__name__

