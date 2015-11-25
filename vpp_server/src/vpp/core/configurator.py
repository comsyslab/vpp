import logging

from vpp.data_acquisition.data_provider import ListeningDataProvider
from vpp.data_acquisition.processing_strategy import DefaultProcessingStrategy
from vpp.database.db_manager import DBManager
from vpp.database.entities.data_acquisition_entities import RabbitMQAdapterEntity, DataProviderEntity
from vpp.util import util


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

        data_adapter_entity = RabbitMQAdapterEntity(host=host, exchange=exchange, queue=queue)

        data_interpreter_fqn = util.get_fully_qualified_name(interpreter_class)
        processing_strategy_fqn = util.get_fully_qualified_name(DefaultProcessingStrategy)
        data_provider_fqn = util.get_fully_qualified_name(ListeningDataProvider)

        data_provider_entity = DataProviderEntity(domain_type=data_provider_fqn,
                                                  data_adapter_entity=data_adapter_entity,
                                                  processing_strategy_domain_type=processing_strategy_fqn,
                                                  data_interpreter_domain_type=data_interpreter_fqn)
        db_manager = DBManager()
        db_manager.persist_entity(data_provider_entity)
        db_manager.close()


    def configure_new_ftp_provider(self, interpreter_class, host="localhost"):
        '''data_adapter_entity = FTPAdapterEntity(host=host, user, pass)

        data_interpreter_fqn = util.get_fully_qualified_name(interpreter_class)
        processing_strategy_fqn = util.get_fully_qualified_name(DefaultProcessingStrategy)
        data_provider_fqn = util.get_fully_qualified_name(ListeningDataProvider)
        interval = 10min

        data_provider_entity = DataProviderEntity(domain_type=data_provider_fqn,
                                                  data_adapter_entity=data_adapter_entity,
                                                  processing_strategy_domain_type=processing_strategy_fqn,
                                                  data_interpreter_domain_type=data_interpreter_fqn)
        db_manager = DBManager()
        db_manager.persist_entity(data_provider_entity)
        db_manager.close()'''
        pass


