import logging
import pickle

from vpp.database.db_manager import DBManager

__author__ = 'ubbe'


class DataProvider(object):

    def __init__(self, dataprovider_entity):

        self.logger = logging.getLogger(__name__)

        self.entity = dataprovider_entity
        self.data_processor = pickle.loads(dataprovider_entity.data_processor_pickled)


    def persist_data_processor(self):
        self.entity.data_processor_pickled = pickle.dumps(self.data_processor)
        db_manager = DBManager()
        db_manager.persist_entity(self.entity)
        db_manager.commit()
        db_manager.close()


    def get_id(self):
        return self.entity.id

    def fetch_and_process_data(self):
        #self.data_processor.fetch_and_process_data()
        self.logger.debug("DataProvider %d fetched and stored data", self.get_id())

