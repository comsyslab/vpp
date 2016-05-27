from vpp.core.configurator import Configurator
from vpp.database.db_manager import DBManager


def recreate_db_schema():
    db_manager = DBManager(autoflush=True)
    db_manager.drop_tables()
    db_manager.create_missing_tables()
    db_manager.close()

if __name__ == '__main__':
    recreate_db_schema()

