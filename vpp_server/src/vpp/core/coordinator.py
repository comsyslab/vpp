from database import db_manager

from vpp.core import DataProviderManager

__author__ = 'ubbe'


class Coordinator:

    def __init__(self):
        self.db_manager = db_manager.instance

        self.dataprovider_manager = DataProviderManager()



if __name__ == '__main__':
    coordinator = Coordinator()