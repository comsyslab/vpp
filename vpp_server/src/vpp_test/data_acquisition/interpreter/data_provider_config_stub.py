class DataProviderConfigStub(object):
    def __init__(self):
        self.ftp_config = FTPConfigStub()


class FTPConfigStub(object):
    def __init__(self):
        self.last_fetch_interpreter = '1970-01-29T00:00:00'