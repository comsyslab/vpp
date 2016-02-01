import StringIO
import logging
import re

import datetime
import iso8601
import pysftp

from vpp.data_acquisition.adapter.abstract_data_adapter import AbstractFetchingAdapter
from vpp.util import util


class SFTPAdapter(AbstractFetchingAdapter):

    def __init__(self, data_provider, data_provider_config):
        super(SFTPAdapter, self).__init__(data_provider, data_provider_config)
        logging.getLogger('paramiko.transport').setLevel('INFO')
        self.ftp_config = data_provider_config.ftp_config

    def fetch_data(self):

        with pysftp.Connection(host=self.ftp_config.host,
                               username=self.ftp_config.username,
                               password=self.ftp_config.password) as sftp:

            remote_dir = self.ftp_config.remote_dir
            file_names = sftp.listdir(remote_dir)

            file_pattern_regex = self.ftp_config.file_pattern
            file_bodies = []

            newest_file_date = util.get_base_date()

            for file_name in file_names:

                if not re.match(file_pattern_regex, file_name) or \
                        not self.file_date_strategy.should_fetch_file(file_name):
                    continue

                string_io = StringIO.StringIO()
                sftp.getfo(remote_dir + '/' + file_name, string_io)
                self.logger.info("Retrieved file " + file_name)
                file_contents = string_io.getvalue()
                file_bodies.append(file_contents)
                file_date = self.file_date_strategy.get_date_from_filename(file_name)
                if file_date > newest_file_date:
                    newest_file_date = file_date

            self.file_date_strategy.update_latest_fetch_date(newest_file_date)

            return file_bodies


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