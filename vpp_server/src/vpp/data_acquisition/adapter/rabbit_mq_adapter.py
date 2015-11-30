import logging
import threading

import pika
from pika.exceptions import ChannelClosed, ConnectionClosed, AMQPConnectionError
from vpp.data_acquisition.adapter.abstract_data_adapter import AbstractListeningAdapter

__author__ = 'ubbe'


class RabbitMQAdapter(AbstractListeningAdapter):

    def __init__(self, entity, data_processor):
        super(RabbitMQAdapter, self).__init__(entity, data_processor)
        self._init_logging()
        self.retry_count = 0

    def _init_logging(self):
        self.logger = logging.getLogger(__name__)
        pika_logger = logging.getLogger('pika')
        pika_logger.setLevel(logging.INFO)

    def _listen_for_data(self):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.entity.host, retry_delay=self.retry_delay_short, connection_attempts=10))
        except (ConnectionClosed, AMQPConnectionError) as e:
            self.logger.debug(e.message)
            self.logger.info("RabbitMQ connect to " + self.entity.host + " failed. Will retry in " + str(self.retry_delay_long) + " seconds.")
            self._schedule_reconnect()
            return

        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.entity.queue)
        self.consumer_tag = self.channel.basic_consume(self._receive_data,
                                                       queue=self.entity.queue,
                                                       no_ack=True)
        try:
            self.channel.start_consuming()
        except ConnectionClosed:
            self.logger.info("RabbitMQAdapter lost connection to " + self.entity.host + ", reconnecting...")
            self._listen_for_data()
        except ChannelClosed:
            self.logger.info("RabbitMQAdapter channel closed.")
        except Exception as e:
            self.logger.exception(e)

    def _receive_data(self, channel, method, properties, body):
        self.logger.info("RabbitMQAdapter " + str(self.entity.id) + " received message: " + body[:100])
        self.data_processor.interpret_and_process_data(str(body))


    def stop(self):
        self.channel.basic_cancel(self.consumer_tag)
        self.channel.stop_consuming()
        self.channel.close()
        self.logger.debug("Listening adapter " + str(self.entity.id) + " cancelled message consumption.")
