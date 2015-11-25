import logging
from ftplib import FTP, error_perm

from vpp.database.entities.data_acquisition_entities import FTPAdapterEntity


class FTPAdapter(object):

    def __init__(self, entity):
        self.logger = logging.getLogger(__name__)
        self.entity = entity


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