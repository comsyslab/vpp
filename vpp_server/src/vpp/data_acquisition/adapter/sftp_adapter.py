import StringIO
import logging
import re

import datetime
import iso8601
import pysftp

from vpp.data_acquisition.adapter.abstract_data_adapter import AbstractFetchingAdapter



class SFTPAdapter(AbstractFetchingAdapter):

    def __init__(self, data_provider, data_provider_config):
        super(SFTPAdapter, self).__init__(data_provider, data_provider_config)
        self.ftp_config = data_provider_config.ftp_config

        logging.getLogger('paramiko.transport').setLevel('INFO')


    def fetch_data(self):

        with pysftp.Connection(host=self.ftp_config.host,
                               username=self.ftp_config.username,
                               password=self.ftp_config.password) as sftp:

            remote_dir = self.ftp_config.remote_dir
            file_names = sftp.listdir(remote_dir)

            file_pattern_regex = self.ftp_config.file_pattern
            file_bodies = []

            newest_file_date = datetime.datetime(1970, 1, 1)


            for file_name in file_names:
                file_date = self.get_file_date(file_name)
                if not re.match(file_pattern_regex, file_name) or \
                   self.date_already_processed(file_date):
                    continue

                string_io = StringIO.StringIO()
                sftp.getfo(remote_dir + '/' + file_name, string_io)
                self.logger.info("Retrieved file " + file_name)
                file_contents = string_io.getvalue()
                file_bodies.append(file_contents)
                if file_date > newest_file_date:
                    newest_file_date = file_date

            if newest_file_date > self.get_last_fetch_date_from_config():
                self.ftp_config.last_fetch = newest_file_date.isoformat()
            return file_bodies

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

    def date_already_processed(self, date):
        return date <= self.get_last_fetch_date_from_config()

    def get_last_fetch_date_from_config(self):
        if not self.ftp_config or \
           not self.ftp_config.last_fetch:
            return datetime.datetime(1970, 1, 1)

        return iso8601.parse_date(self.ftp_config.last_fetch, default_timezone=None)


if __name__ == '__main__':

    class SFTPConfig(object):
        def __init__(self):
            self.host = 'localhost'
            self.password = 'ftp4ccess'
            self.username = 'ftp_user'
            self.remote_dir = 'nrgi'
            self.file_pattern = 'nrgi_absolute-571313115104260611-201[0-9-]+\.xml'
            self.last_fetch = None

    class DataProviderConfig(object):
        def __init__(self):
            self.ftp_config = SFTPConfig()


    sftp = SFTPAdapter(None, DataProviderConfig())

    data = sftp.fetch_data()

    print data