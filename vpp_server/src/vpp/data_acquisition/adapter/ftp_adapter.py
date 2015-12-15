from ftplib import FTP, error_perm


from vpp.data_acquisition.adapter.abstract_data_adapter import AbstractFetchingAdapter



class FTPAdapter(AbstractFetchingAdapter):

    def __init__(self, data_provider, ftp_config):
        super(FTPAdapter, self).__init__(data_provider, ftp_config)
        self.ftp_config = ftp_config


    def fetch_data(self):
        self.message = ''
        ftp = FTP(self.ftp_config.host)
        try :
            ftp.login(self.ftp_config.username, self.ftp_config.password)
        except error_perm as e:
            self.logger.exception(e)

        command = 'RETR ' + self.ftp_config.file
        response_code = ''
        try:
            response_code = ftp.retrlines(command, self.receive_line)
        except error_perm as e:
            self.logger.exception(e)

        if response_code.startswith('226'):
            self.logger.debug("FTPAdapter fetched file " + self.ftp_config.file + " from " + self.ftp_config.host)

        return self.message

    def receive_line(self, line):
        decoded_line = line.decode(self.ftp_config.encoding) + '\n'
        self.message += decoded_line


class FTPConfigTest(object):
    def __init__(self):
        self.host = 'localhost'
        self.username = 'ftp_user'
        self.password = 'ftp4ccess'
        self.file = 'file.txt'

if __name__ == '__main__':
    #quick test
    config = FTPConfigTest()
    ftp_adapter = FTPAdapter(None, config)
    print ftp_adapter.fetch_data()