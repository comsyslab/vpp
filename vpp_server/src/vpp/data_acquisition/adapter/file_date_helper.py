import re

import datetime

import iso8601


class FileDateHelper(object):

    def __init__(self, ftp_config):
        self.ftp_config = ftp_config

    def get_file_date(self, file_name):
        regex = '.*-([0-9]{4})-([0-1][0-9])-([0-3][0-9])\..{3}' #Any string ending in a date "yyyy-mm-dd" before the 3-char file extension.

        result = re.match(regex, file_name)
        if not result:
            return None

        year = int(result.groups()[0])
        month = int(result.groups()[1])
        day = int(result.groups()[2])

        date = datetime.datetime(year, month, day)
        return date

    def file_already_processed(self, file_name):
        return self.date_already_processed(self.get_file_date(file_name))

    def date_already_processed(self, date):
        return date <= self._get_last_fetch_date_from_config()

    def _get_last_fetch_date_from_config(self):
        if not self.ftp_config or \
           not self.ftp_config.last_fetch:
            return datetime.datetime(1970, 1, 1)

        return iso8601.parse_date(self.ftp_config.last_fetch, default_timezone=None)

    def update_newest_filename(self, newest_filename):
        self.update_newest_file(self.get_file_date(newest_filename))

    def update_newest_file(self, new_date):
        if new_date > self._get_last_fetch_date_from_config():
            self.ftp_config.last_fetch = new_date.isoformat()