import logging
import re

import datetime

import iso8601


class FileDateHelper(object):

    def __init__(self, ftp_config, component='adapter'):
        self.logger = logging.getLogger(__name__)
        self.component = component
        self.ftp_config = ftp_config
        self.base_date = datetime.datetime(1970, 1, 1)


    def get_file_date(self, file_name, modified_date=None):
        mode = self.ftp_config.file_date_mode
        if mode == 'filename':
            return self.get_date_from_filename(file_name)
        elif mode == 'modified':
            return modified_date
        else:
            self.logger.error('No property \'file_date_mode\' in config file. Using last modification date.')
            return modified_date

    def get_date_from_filename(self, file_name):
        regex = self.ftp_config.file_pattern
        result = re.match(regex, file_name)
        if not result:
            return None
        if len(result.groups()) == 0:
            return None
        year = self.get_element(result.groups(), 0)
        if year < 100:
            year = year + 2000

        month = self.get_element(result.groups(), 1, default_value=1)
        day = self.get_element(result.groups(), 2, default_value=1)
        hour = self.get_element(result.groups(), 3)

        date = datetime.datetime(year, month, day, hour)
        return date

    def get_element(self, list, index, default_value=0):
        if len(list) > index:
            try:
                return int(list[index])
            except:
                return default_value
        return default_value

    def file_already_processed(self, file_name, modified_date=None):
        file_date = self.get_file_date(file_name, modified_date)
        already_processed = self.date_already_processed(file_date)
        return already_processed

    def date_already_processed(self, date):
        if date is None:
            return False

        if self.component == 'adapter' and self.ftp_config.fetch_again_when_date_equal:
            return date < self._get_last_fetch_date_from_config()

        return date <= self._get_last_fetch_date_from_config()

    def update_latest_fetch_for_file(self, newest_filename, modified_date=None):
        self.update_latest_fetch_date(self.get_file_date(newest_filename, modified_date))

    def update_latest_fetch_date(self, new_date):

        if new_date is None:
            return
        try:
            if new_date <= self._get_last_fetch_date_from_config():
                return
        except Exception as e:
            self.logger.exception(e)

        if self.component == 'interpreter':
            self.ftp_config.last_fetch_interpreter = new_date.isoformat()
        else:
            self.ftp_config.last_fetch_adapter = new_date.isoformat()

    def _get_last_fetch_date_from_config(self):
        if self.component == 'interpreter':
            return self.get_fetch_date_interpreter()
        else:
            return self.get_fetch_date_adapter()

    def get_fetch_date_interpreter(self):
        if not self.ftp_config.last_fetch_interpreter:
            return self.base_date
        return iso8601.parse_date(self.ftp_config.last_fetch_interpreter, default_timezone=None)

    def get_fetch_date_adapter(self):
        if not self.ftp_config.last_fetch_adapter:
            return self.base_date
        date_from_config = iso8601.parse_date(self.ftp_config.last_fetch_adapter, default_timezone=None)
        return date_from_config