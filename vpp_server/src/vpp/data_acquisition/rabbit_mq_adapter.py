import logging
import threading
from abc import ABCMeta, abstractmethod

import pika
import time
from pika.exceptions import ChannelClosed, ConnectionClosed

__author__ = 'ubbe'

class AbstractListeningAdapter(object):

    __metaclass__ = ABCMeta

    def __init__(self, entity, data_processor):
        self.logger = logging.getLogger(__name__)
        self.entity = entity
        self.data_processor = data_processor

    def start(self):
        self.thread = threading.Thread(target=self._listen_for_data, args=(), name=__name__)
        self.thread.setDaemon(True)
        self.thread.start()

    def stop(self):
        self.channel.basic_cancel(self.consumer_tag)
        self.channel.stop_consuming()
        self.logger.debug("RabbitMQAdapter " + str(self.entity.id) + " cancelled message consumption.")

    def join(self):
        self.logger.debug("Joining data adapter " + str(self.data_adapter) + "...")
        begin = time.time()
        self.thread.join()
        time_spent = time.time() - begin
        self.logger.debug("...joined in "  + str(time_spent))

    @abstractmethod
    def _listen_for_data(self):
        pass


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