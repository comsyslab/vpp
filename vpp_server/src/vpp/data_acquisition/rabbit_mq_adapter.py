import logging

import pika
from pika.exceptions import ChannelClosed

__author__ = 'ubbe'

class RabbitMQAdapter(object):

    def __init__(self, entity):
        self.entity = entity
        self.init_logging()

    def init_logging(self):
        self.logger = logging.getLogger(__name__)
        pika_logger = logging.getLogger('pika')
        pika_logger.setLevel(logging.INFO)

    def listen_for_data(self, callback):
        self.callback = callback
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.entity.host))

        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.entity.queue)
        self.consumer_tag = self.channel.basic_consume(self.receive_data,
                                                       queue=self.entity.queue,
                                                       no_ack=True)
        try:
            self.channel.start_consuming()
        except ChannelClosed:
            self.logger.debug("RabbitMQAdapter channel closing.")
        self.logger.debug("RabbitMQAdapter exited.")


    def stop_listening(self):
        self.channel.basic_cancel(self.consumer_tag)
        self.channel.stop_consuming()
        self.logger.debug("RabbitMQAdapter cancelled message consumption.")

    def receive_data(self, channel, method, properties, body):
        self.logger.debug("RabbitMQAdapter " + str(self.entity.id) + " received message: " + str(body))
        self.callback(body)