from core.data_provider_manager import DataProviderManager
from database import db_manager
from database.db_manager import DBManager

__author__ = 'ubbe'


class Coordinator:

    def __init__(self):
        self.db_manager = db_manager.instance

        self.dataprovider_manager = DataProviderManager()



if __name__ == '__main__':
    coordinator = Coordinator()