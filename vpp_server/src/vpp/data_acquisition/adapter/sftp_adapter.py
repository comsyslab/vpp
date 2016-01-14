import StringIO
import re

import pysftp

from vpp.data_acquisition.adapter.abstract_data_adapter import AbstractFetchingAdapter



class SFTPAdapter(AbstractFetchingAdapter):

    def __init__(self, data_provider, data_provider_config):
        super(SFTPAdapter, self).__init__(data_provider, data_provider_config)
        self.ftp_config = data_provider_config.ftp_config


    def fetch_data(self):

        with pysftp.Connection(host=self.ftp_config.host,
                               username=self.ftp_config.username,
                               password=self.ftp_config.password) as sftp:

            remote_dir = self.ftp_config.remote_dir
            file_names = sftp.listdir(remote_dir)

            regex_string = self.ftp_config.file_pattern
            file_bodies = []
            for file_name in file_names:
                if re.match(regex_string, file_name):
                    string_io = StringIO.StringIO()
                    sftp.getfo(remote_dir + '/' + file_name, string_io)
                    file_contents = string_io.getvalue()
                    file_bodies.append(file_contents)
            return file_bodies


if __name__ == '__main__':

    class SFTPConfig(object):
        def __init__(self):
            self.host = 'localhost'
            self.password = 'ftp4ccess'
            self.username = 'ftp_user'
            self.remote_dir = 'nrgi'
            self.file_pattern = 'nrgi_(absolute|delta)-571313115104260611-2014-03-04.xml'

    class DataProviderConfig(object):
        def __init__(self):
            self.ftp_config = SFTPConfig()


    sftp = SFTPAdapter(None, DataProviderConfig())

    data = sftp.fetch_data()

    print data