import logging

import pika
from pika.exceptions import ChannelClosed, ConnectionClosed

from vpp.data_acquisition.adapter.abstract_data_adapter import AbstractListeningAdapter

__author__ = 'ubbe'


class RabbitMQAdapter(AbstractListeningAdapter):

    def __init__(self, entity, data_processor):
        super(RabbitMQAdapter, self).__init__(entity, data_processor)
        self._init_logging()

    def _init_logging(self):
        self.logger = logging.getLogger(__name__)
        pika_logger = logging.getLogger('pika')
        pika_logger.setLevel(logging.INFO)

    def _listen_for_data(self):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(self.entity.host))
        except ConnectionClosed as e:
            self.logger.exception(e)
            return

        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.entity.queue)
        self.consumer_tag = self.channel.basic_consume(self._receive_data,
                                                       queue=self.entity.queue,
                                                       no_ack=True)
        try:
            self.channel.start_consuming()
        except ChannelClosed:
            self.logger.debug("RabbitMQAdapter channel closing.")
        self.logger.debug("RabbitMQAdapter exited.")

    def _receive_data(self, channel, method, properties, body):
        self.logger.info("RabbitMQAdapter " + str(self.entity.id) + " received message: " + str(body))
        self.data_processor.interpret_and_process_data(str(body))