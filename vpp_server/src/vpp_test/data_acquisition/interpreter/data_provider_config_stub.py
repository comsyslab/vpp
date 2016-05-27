class DataProviderConfigStub(object):
    def __init__(self, id_prefix="test"):
        self.ftp_config = FTPConfigStub()
        self.id_prefix = id_prefix


class FTPConfigStub(object):
    def __init__(self):
        self.last_fetch_interpreter = '1970-01-29T00:00:00'