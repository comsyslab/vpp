from vpp.config.config_ini_parser import ConfigIniParser
from vpp.core.configurator import Configurator
from vpp.database.db_manager import DBManager


def recreate_db_schema():
    db_manager_dw = DBManager(ConfigIniParser().get_db_string('DB-DW'))
    db_manager_dw.drop_tables()
    db_manager_dw.create_missing_tables()
    db_manager_dw.close()

if __name__ == '__main__':
    recreate_db_schema()

