"""
rabbitMQ.py
This python library contains class definitions related to RabbitMQ: exchange, queues and ssl options.
Version 1.0
Date 11/12/2013
Developed by Sergi Rotger Griful <srgr@iha.dk>
"""

from ConfigParser import SafeConfigParser
import string


class Exchange(object):
    """RabbitMQ Exchange class from a .ini file"""

    def __init__(self, f_name='resource/configuration.ini'):
        parser = SafeConfigParser()
        parser.read(f_name)
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

    def __init__(self, f_name='resource/configuration.ini'):
        parser = SafeConfigParser()
        parser.read(f_name)
        self.name = parser.get('queue', 'name')
        aux = parser.get('queue', 'routingKey')
        self.routingKey = aux.split(',')
        self.durable = bool(parser.get('queue', 'durable') == 'True')
        self.exclusive = bool(parser.get('queue', 'exclusive') == 'True')
        self.autoDelete = bool(parser.get('queue', 'autoDelete') == 'True')


class SslOptions(object):
    """SSL options to connect top  the RabbitMQ server"""

    def __init__(self, f_name='resource/configuration.ini'):
        parser = SafeConfigParser()
        parser.read(f_name)
        self.dict = {'cacerts': parser.get('ssl_options', 'cacerts'),
                     'certfile': parser.get('ssl_options', 'certfile'),
                     'keyfile': parser.get('ssl_options', 'keyfile'),
                     'cert_reqs': parser.get('ssl_options', 'cert_reqs'),
                     'fail_if_no_peer_cert': parser.get('ssl_options', 'fail_if_no_peer_cert')}
