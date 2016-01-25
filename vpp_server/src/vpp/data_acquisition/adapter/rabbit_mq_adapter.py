import logging
import threading

import pika
from pika.exceptions import ChannelClosed, ConnectionClosed, AMQPConnectionError, IncompatibleProtocolError
from vpp.data_acquisition.adapter.abstract_data_adapter import AbstractListeningAdapter

__author__ = 'ubbe'


class RabbitMQAdapter(AbstractListeningAdapter):

    def __init__(self, data_provider, data_provider_config):
        super(RabbitMQAdapter, self).__init__(data_provider)
        self._init_logging()
        self.retry_count = 0
        self.exchange = data_provider_config.rabbitmq_exchange
        self.queue = data_provider_config.rabbitmq_queue
        self.ssl_options = data_provider_config.rabbitmq_ssl_options


    def _init_logging(self):
        self.logger = logging.getLogger(__name__)
        pika_logger = logging.getLogger('pika')
        pika_logger.setLevel('INFO')

    def _listen_for_data(self):
        self._init_connection()
        if self.connection is None:
            return
        self._init_channel()
        self._declare_queue()
        self._bind_queue()
        self._consume()

    def _init_connection(self):
        self.connection = None
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.exchange.url, credentials=self.get_credentials(),
                                          ssl=self.exchange.ssl, port=self.exchange.port))
        except IncompatibleProtocolError as e:
            self.logger.exception('IncompatibleProtocolError ' + e.message)
        except (ConnectionClosed, AMQPConnectionError) as e:
            self.logger.debug(e.message)
            self.logger.info("RabbitMQ connect to " + self.exchange.url + " failed. Will retry in " + str(
                self.retry_delay_long) + " seconds.")
            self._schedule_reconnect()
        self.logger.debug("RabbitMQ connect to " + self.exchange.url + " successful.")

    def _consume(self):
        self.consumer_tag = self.channel.basic_consume(self._receive_data,
                                                       queue=self.queue.name,
                                                       no_ack=True)

        try:
            self.channel.start_consuming()
        except ConnectionClosed:
            self.logger.info("RabbitMQAdapter lost connection to " + self.exchange.url + ", reconnecting...")
            self._listen_for_data()
        except ChannelClosed:
            self.logger.info("RabbitMQAdapter channel closed.")
        except Exception as e:
            self.logger.exception(e)
        self.logger.debug("Listening adapter " + str(self.exchange.url) + " exited.")

    def _bind_queue(self):
        for routing_key in self.queue.routing_keys:
            self.channel.queue_bind(exchange=self.exchange.name, queue=self.queue.name, routing_key=routing_key)
            self.logger.debug("queue_bind(exchange=" + self.exchange.name + ", queue=" + self.queue.name + ", routing_key=" + routing_key + ")")

    def _declare_queue(self):
        result = self.channel.queue_declare(queue=self.queue.name, exclusive=self.queue.exclusive,
                                            durable=self.queue.durable, auto_delete=self.queue.auto_delete)
        if result.method.NAME != 'Queue.DeclareOk':
            self.logger.error("Error declaring RabbitMQ queue: " + str(result.method.NAME))

    def _init_channel(self):
        self.channel = self.connection.channel()
        result = self.channel.exchange_declare(exchange=self.exchange.name, type=self.exchange.type,
                                               durable=self.exchange.durable)
        if result.method.NAME != 'Exchange.DeclareOk':
            self.logger.error("Error declaring RabbitMQ exchange: " + str(result.method.NAME))

    def get_credentials(self):
        if (self.exchange.username):
            return pika.PlainCredentials(self.exchange.username, self.exchange.password)
        return None

    def _receive_data(self, channel, method, properties, body):
        self.logger.info("RabbitMQAdapter " + str(self.exchange.url) + " received message: " + body[:100])
        self.data_provider.interpret_and_process_data([str(body)])


    def stop(self):
        self.logger.debug("Listening adapter " + str(self.exchange.url) + " cancelling message consumption...")
        self.channel.basic_cancel(self.consumer_tag)
        self.channel.stop_consuming()
        self.channel.close()
        self.logger.debug("Listening adapter " + str(self.exchange.url) + " cancelled message consumption.")
