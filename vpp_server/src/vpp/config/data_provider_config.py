import logging
from ConfigParser import SafeConfigParser


class DataProviderConfig(object):

    def __init__(self, file_name):
        self.logger = logging.getLogger(__name__)
        self.config_parser = ErrorHandlingConfigParser(SafeConfigParser())
        self._parse_file(file_name)

    def _parse_file(self, file_name):
        ok_list = self.config_parser.read(file_name)
        if len(ok_list) == 0:
            self.logger.error("Could not read config file " + file_name)

    @property
    def adapter_fqn(self):
        return self.config_parser.get('data_provider', 'adapter')

    @property
    def interpreter_fqn(self):
        return self.config_parser.get('data_provider', 'interpreter')

    @property
    def processor_fqn(self):
        return self.config_parser.get('data_provider', 'processor')

    @property
    def rabbitmq_exchange(self):
        return Exchange(self.config_parser)

    @property
    def rabbitmq_queue(self):
        return Queue(self.config_parser)

    @property
    def rabbitmq_ssl_options(self):
        return SslOptions(self.config_parser)

    @property
    def ftp_config(self):
        return FTPConfig(self.config_parser)


class ErrorHandlingConfigParser(object):
    def __init__(self, config_parser):
        self.parser = config_parser

    def get(self, section_name, property_name):
        try:
            return self.parser.get(section_name, property_name)
        except:
            return None

    def read(self, file_name):
        self.file_name = file_name
        return self.parser.read(file_name)

    def set(self, section, option, value=None):
        self.parser.set(section, option, value)
        file = open(self.file_name, mode='w')
        self.parser.write(file)
        file.close()


class Exchange(object):
    """RabbitMQ Exchange class from a .ini file"""

    def __init__(self, parser):
        self.username = parser.get('exchange', 'username')
        self.password = parser.get('exchange', 'password')
        self.url = parser.get('exchange', 'serverAddressList')
        self.port = int(parser.get('exchange', 'port'))
        self.ssl = bool(parser.get('exchange', 'sslConnection') == 'True')
        self.name = parser.get('exchange', 'exchangeName')
        self.durable = bool(parser.get('exchange', 'exchangeDurable') == 'True')
        self.type = parser.get('exchange', 'exchangeType')


class Queue(object):
    """RabbitMQ Queue class from a .ini file"""

    def __init__(self, parser):
        self.name = parser.get('queue', 'name')
        keys_string = parser.get('queue', 'routingKeys')
        self.routing_keys = keys_string.split(',')
        self.durable = bool(parser.get('queue', 'durable') == 'True')
        self.exclusive = bool(parser.get('queue', 'exclusive') == 'True')
        self.auto_delete = bool(parser.get('queue', 'autoDelete') == 'True')


class SslOptions(object):
    """SSL options to connect to the RabbitMQ server"""

    def __init__(self, parser):
        self.dict = {'cacerts': parser.get('ssl_options', 'cacerts'),
                     'certfile': parser.get('ssl_options', 'certfile'),
                     'keyfile': parser.get('ssl_options', 'keyfile'),
                     'cert_reqs': parser.get('ssl_options', 'cert_reqs'),
                     'fail_if_no_peer_cert': parser.get('ssl_options', 'fail_if_no_peer_cert')}


class FetchingAdapterConfig(object):
    def __init__(self, parser):
        self.parser = parser
        self.interval = parser.get('fetch', 'interval')
        self.fetch_again_when_date_equal = bool(parser.get('fetch', 'fetch_again_when_date_equal'))
        self.adapter_date_strategy_fqn =  parser.get('fetch', 'adapter_date_strategy')
        try:
            self.fetch_again_hours = int(parser.get('fetch', 'fetch_again_hours'))
        except:
            self.fetch_again_hours = 0

    @property
    def last_fetch_adapter(self):
        return self.parser.get('fetch', 'last_fetch_adapter')

    @last_fetch_adapter.setter
    def last_fetch_adapter(self, value):
        self.parser.set('fetch', 'last_fetch_adapter', value)

    @property
    def last_fetch_interpreter(self):
        return self.parser.get('fetch', 'last_fetch_interpreter')

    @last_fetch_interpreter.setter
    def last_fetch_interpreter(self, value):
        self.parser.set('fetch', 'last_fetch_interpreter', value)


class FTPConfig(FetchingAdapterConfig):
    def __init__(self, parser):
        super(FTPConfig, self).__init__(parser)

        section_name = 'ftp'
        self.username = parser.get(section_name, 'username')
        self.password = parser.get(section_name, 'password')
        self.host = parser.get(section_name, 'host')
        self.remote_dir = parser.get(section_name, 'remote_dir')
        self.file_pattern = parser.get(section_name, 'file_pattern')
        self.encoding = parser.get(section_name, 'encoding')
