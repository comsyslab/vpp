import calendar
from _socket import gaierror
from ftplib import FTP, error_perm

import re

import datetime

from vpp.data_acquisition.adapter.abstract_data_adapter import AbstractFetchingAdapter



class FTPAdapter(AbstractFetchingAdapter):

    def __init__(self, data_provider, data_provider_config):
        super(FTPAdapter, self).__init__(data_provider, data_provider_config)
        self.ftp_config = data_provider_config.ftp_config


    def fetch_data(self):
        self.logger.debug("FTPAdapter fetching %s", self.ftp_config.file_pattern)
        file_bodies = []
        try:
            self.ftp = FTP(self.ftp_config.host)
            self.ftp.login(self.ftp_config.username, self.ftp_config.password)
        except (error_perm, gaierror) as e:
            self.logger.exception(e)
            return file_bodies

        if self.ftp_config.remote_dir:
            self.ftp.cwd(self.ftp_config.remote_dir)

        self.retrieve_file_list_simple()

        regex_string = self.ftp_config.file_pattern

        for file_name in self.files:

            if not re.match(regex_string, file_name):
                self.logger.debug('File ' + file_name + ' skipped. did not match regex ' + regex_string)
                continue

            if not self.file_date_strategy.should_fetch_file(file_name):
                self.logger.debug('File ' + file_name + ' skipped since it is already processed.')
                continue

            self._file_contents = ''
            self.retrieve_file(file_name)

            file_bodies.append(self._file_contents)

            self.file_date_strategy.update_latest_fetch_for_file(file_name)
        self.logger.debug('FTPAdapter returned ' + str(len(file_bodies)) + ' file bodies.')
        return file_bodies

    def retrieve_file_list_simple(self):
        self.files = []
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
        if file_name.startswith('./'):
            file_name = file_name[2:]
        self.files.append(file_name)

    def _receive_line(self, line):
        decoded_line = line.decode(self.ftp_config.encoding) + '\n'
        self._file_contents += decoded_line
