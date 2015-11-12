__author__ = 'ubbe'

class RabbitMQAdapter(object):

    def __init__(self, entity):
        self.entity = entity
        self.host = ""
        self.exchange = ""
        self.queue = ""

    def fetch_data(self):
        return '{"version":3, "timestamp":"2014-10-08T09:30:32.750Z",' \
               '"reading":[{"sensorId":1152,"appartmentId":3,"value":1024.0,"timestamp":"2014-10-08T09:30:32.747Z"}]' \
               '}'