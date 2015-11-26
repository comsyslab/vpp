import logging
from abc import ABCMeta, abstractmethod
from ftplib import FTP, error_perm

from vpp.data_acquisition.data_provider_timer import DataProviderTimer
from vpp.database.entities.data_acquisition_entities import FTPAdapterEntity


class AbstractFetchingAdapter(object):

    __metaclass__ = ABCMeta

    def __init__(self, entity):
        self.logger = logging.getLogger(__name__)
        self.entity = entity

    def get_interval(self):
        return self.entity.interval

    def start(self):
        self.timer = DataProviderTimer(self)
        self.timer.start()

    def stop(self):
        self.timer.stop()

    def join(self):
        self.timer.join()

    def fetch_and_process_data(self, db_manager=None):
        data = self.data_adapter.fetch_data()
        self.data_processor.interpret_and_process_data(data, db_manager)

    @abstractmethod
    def fetch_data(self):
        pass


class FTPAdapter(object):

    def fetch_data(self):
        self.message = ''
        ftp = FTP(self.entity.host)
        try :
            ftp.login(self.entity.user, self.entity.password)
        except error_perm as e:
            self.logger.exception(e)

        command = 'RETR ' + self.entity.file
        response_code = ''
        try:
            response_code = ftp.retrlines(command, self.receive_line)
        except error_perm as e:
            self.logger.exception(e)

        if response_code.startswith('226'):
            print "FTPAdapter " + str(self.entity.id) + " fetched file " + self.entity.file + " from " + self.entity.host

    def receive_line(self, line):
        self.message += line + '\n'


if __name__ == '__main__':
    #quick test
    entity = FTPAdapterEntity(host='localhost', user='ftp_user', password='ftp_pass', file='file.txt')
    ftp_adapter = FTPAdapter(entity)
    ftp_adapter.fetch_data()