from ftplib import FTP, error_perm

from vpp.data_acquisition.adapter.abstract_data_adapter import AbstractFetchingAdapter



class FTPAdapter(AbstractFetchingAdapter):

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
            self.logger.info("FTPAdapter " + str(self.entity.id) + " fetched file " + self.entity.file + " from " + self.entity.host)

        return self.message

    def receive_line(self, line):
        self.message += line + '\n'


if __name__ == '__main__':
    #quick test
    #entity = FTPAdapterEntity(host='localhost', user='ftp_user', password='ftp_pass', file='file.txt')
    #ftp_adapter = FTPAdapter(entity)
    #ftp_adapter.fetch_data()
    pass