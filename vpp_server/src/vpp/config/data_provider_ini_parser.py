import logging
from ConfigParser import SafeConfigParser


class DataProviderIniParser(object):

    def __init__(self, file_name):
        self.logger = logging.getLogger(__name__)
        self.safe_config_parser = SafeConfigParser()
        self.parse_file(file_name)

    def parse_file(self, file_name):
        ok_list = self.safe_config_parser.read(file_name)
        if len(ok_list) == 0:
            self.logger.error("Could not read config file " + file_name)

    def get_adapter_fqn(self):
        return self.safe_config_parser.get('data_provider', 'adapter')

    def get_interpreter_fqn(self):
        return self.safe_config_parser.get('data_provider', 'interpreter')

    def get_processor_fqn(self):
        return self.safe_config_parser.get('data_provider', 'processor')

    def get_rabbitmq_exchange(self):
        return Exchange(self.safe_config_parser)

    def get_rabbitmq_queue(self):
        return Queue(self.safe_config_parser)

    def get_rabbitmq_ssl_options(self):
        return SslOptions(self.safe_config_parser)

    def get_ftp_config(self):
        return FTPConfig(self.safe_config_parser)



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


class FTPConfig(object):
    def __init__(self, parser):
        section_name = 'ftp'
        self.username = parser.get(section_name, 'username')
        self.password = parser.get(section_name, 'password')
        self.host = parser.get(section_name, 'host')
        self.port = int(parser.get(section_name, 'port'))
        self.file = parser.get(section_name, 'file')
