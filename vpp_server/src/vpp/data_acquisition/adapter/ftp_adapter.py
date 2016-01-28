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

            if self.file_date_helper.file_already_processed(file_name):
                self.logger.debug('File ' + file_name + ' skipped since it is already processed.')
                continue

            self._file_contents = ''
            self.retrieve_file(file_name)

            file_bodies.append(self._file_contents)

            self.file_date_helper.update_latest_fetch_for_file(file_name)
        self.logger.debug('FTPAdapter returned ' + str(len(file_bodies)) + ' file bodies.')
        return file_bodies

    def retrieve_file_list_simple(self):
        self.files = []
        command = 'NLST'
        try:
            response_code = self.ftp.retrlines(command, self._receive_file_name)
        except Exception as e:
            self.logger.exception(e)

    def retrieve_file_list_detailed(self):
        self.files = []
        try:
            self.ftp.dir(self._receive_file_entry)
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

    def _receive_file_entry(self, file_entry):
        if file_entry.startswith('total'):
            return
        elements = file_entry.split()
        file_name_index = len(elements) - 1
        time_index = file_name_index - 1
        day_index = time_index - 1
        month_name_index = day_index - 1

        file_name = elements[file_name_index]

        if file_name == '.' or file_name == '..':
            return

        if file_name.startswith('./'):
            file_name = file_name[2:]

        time = elements[time_index]

        if time.find(':') > 0:
            hour = int(time.split(':')[0])
            minute = int(time.split(':')[1])
            year = datetime.datetime.now().year
        else:
            year = int(time)
            hour = 0
            minute = 0

        day = int(elements[day_index])
        month_name = elements[month_name_index]
        month = list(calendar.month_abbr).index(month_name)

        modified_date = datetime.datetime(year, month, day, hour, minute)
        if modified_date > datetime.datetime.now():
            modified_date.year = modified_date.year - 1

        self.files.append((file_name, modified_date))

    def _receive_line(self, line):
        decoded_line = line.decode(self.ftp_config.encoding) + '\n'
        self._file_contents += decoded_line
