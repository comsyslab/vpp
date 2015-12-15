from abc import ABCMeta, abstractmethod


class AbstractDataInterpreter(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def interpret_data(self, data_string):
        pass