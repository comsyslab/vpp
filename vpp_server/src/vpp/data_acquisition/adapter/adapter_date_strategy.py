import datetime
import logging
import re

import iso8601


class DefaultAdapterFileDateStrategy(object):

    def __init__(self, ftp_config):
        self.logger = logging.getLogger(__name__)
        self.ftp_config = ftp_config
        self.base_date = datetime.datetime(1970, 1, 1)

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

    def update_latest_fetch_for_file(self, newest_filename):
        self.update_latest_fetch_date(self.get_date_from_filename(newest_filename))

    def should_fetch_file(self, file_name):
        file_date = self.get_date_from_filename(file_name)
        return self.should_process_date(file_date)

    def should_process_date(self, date):
        if date is None:
            return False

        if self.ftp_config.fetch_again_hours > 0:
            delta = datetime.timedelta(hours=self.ftp_config.fetch_again_hours)
            date = date + delta

        if self.ftp_config.fetch_again_when_date_equal:
            return date >= self._get_last_fetch_date_from_config()

        return date > self._get_last_fetch_date_from_config()

    def update_latest_fetch_date(self, new_date):
        if new_date is None:
            return

        if new_date <= self._get_last_fetch_date_from_config():
            return

        self.ftp_config.last_fetch_adapter = new_date.isoformat()

    def _get_last_fetch_date_from_config(self):
        if not self.ftp_config.last_fetch_adapter:
            return self.base_date
        date_from_config = iso8601.parse_date(self.ftp_config.last_fetch_adapter, default_timezone=None)
        return date_from_config


class CO2FileDateStrategy(DefaultAdapterFileDateStrategy):

    def should_process_date(self, date):
        today = datetime.datetime.now().date()
        day_to_check = date.date()
        return day_to_check >= today