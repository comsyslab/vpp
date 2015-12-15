import logging
from ConfigParser import SafeConfigParser


class ConfigIniParser(object):

    def __init__(self, file_name='../resources/config.ini'):
        self.logger = logging.getLogger(__name__)
        self.config_parser = SafeConfigParser()
        self._load_file(file_name)

    def _load_file(self, file_name):
        ok_list = self.config_parser.read(file_name)
        if len(ok_list) == 0:
            self.logger.error("Could not read config file " + file_name)

    def get_db_string(self):
        section_name = 'DB'

        user = self.config_parser.get(section_name, 'user')
        password = self.config_parser.get(section_name, 'password')
        host = self.config_parser.get(section_name, 'host')
        database_name = self.config_parser.get(section_name, 'database')
        db_string = "postgresql://" + user + ":" + password + "@" + host + "/" + database_name
        return db_string

    def get_measurement_partition_period(self):
        return int(self.config_parser.get('DB', 'measurement_partition_period_hours'))

    def get_rolling_window_length(self):
        return int(self.config_parser.get('DB', 'rolling_window_length_days'))

    def get_log_level(self):
        return self.config_parser.get('LOG', 'level')

