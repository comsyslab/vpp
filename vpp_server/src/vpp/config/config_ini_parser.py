import logging
from ConfigParser import SafeConfigParser


class ConfigIniParser(object):

    def __init__(self, file_name='../resources/config.ini'):
        self.logger = logging.getLogger(__name__)
        self.config_ini_parser = SafeConfigParser()
        self.parse_file(file_name)

    def parse_file(self, file_name):
        ok_list = self.config_ini_parser.read(file_name)
        if len(ok_list) == 0:
            self.logger.error("Could not read config file " + self.ini_file_name)

    def get_db_string(self):
        section_name = 'DB'

        if not self.config_ini_parser.has_section(section_name):
            self.logger.error("No section '" + section_name + "' in config file " + self.ini_file_name)

        user = self.config_ini_parser.get(section_name, 'user')
        password = self.config_ini_parser.get(section_name, 'password')
        host = self.config_ini_parser.get(section_name, 'host')
        database_name = self.config_ini_parser.get(section_name, 'database')
        db_string = "postgresql://" + user + ":" + password + "@" + host + "/" + database_name
        return db_string
