from ftplib import FTP, error_perm

import re

from vpp.data_acquisition.adapter.abstract_data_adapter import AbstractFetchingAdapter



class FTPAdapter(AbstractFetchingAdapter):

    def __init__(self, data_provider, ftp_config):
        super(FTPAdapter, self).__init__(data_provider, ftp_config)
        self.ftp_config = ftp_config


    def fetch_data(self):
        self.ftp = FTP(self.ftp_config.host)
        try :
            self.ftp.login(self.ftp_config.username, self.ftp_config.password)
        except error_perm as e:
            self.logger.exception(e)

        self.retrieve_file_list()

        regex_string = self.ftp_config.file_pattern
        file_bodies = []
        for file_name in self.file_names:
            if re.match(regex_string, file_name):
                self._file_contents = ''
                self.retrieve_file(file_name)
                file_bodies.append(self._file_contents)

        return file_bodies

    def retrieve_file_list(self):
        self.file_names = []
        command = 'NLST'
        try:
            response_code = self.ftp.retrlines(command, self._receive_file_name)
        except Exception as e:
            self.logger.exception(e)

    def retrieve_file(self, file):
        command = 'RETR ' + file
        response_code = ''
        try:
            response_code = self.ftp.retrlines(command, self._receive_line)
        except error_perm as e:
            self.logger.exception(e)
        if response_code.startswith('226'):
            self.logger.debug("FTPAdapter fetched file " + file + " from " + self.ftp_config.host)

    def _receive_file_name(self, file_name):
        self.file_names.append(file_name)

    def _receive_line(self, line):
        decoded_line = line.decode(self.ftp_config.encoding) + '\n'
        self._file_contents += decoded_line


class FTPConfigTest(object):
    def __init__(self):
        self.host = 'localhost'
        self.username = 'ftp_user'
        self.password = 'ftp4ccess'
        self.file = 'file.txt'

if __name__ == '__main__':
    #quick test
    pass
    #config = FTPConfigTest()
    #ftp_adapter = FTPAdapter(None, config)
    #print ftp_adapter.fetch_data()