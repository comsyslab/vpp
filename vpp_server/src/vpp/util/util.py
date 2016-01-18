import logging
import multiprocessing
import threading

import datetime

from vpp.config.config_ini_parser import ConfigIniParser


def get_fully_qualified_name(cls):
    return cls.__module__ + "." + cls.__name__

def get_thread_info():
    return multiprocessing.current_process().name + " " + threading.currentThread().name + " "

def secs_to_ms(secs):
    return int(secs*1000)

def init_strptime():
    datetime.datetime.strptime('', '')


def load_and_set_log_level():
    root_logger = logging.getLogger()
    current_level = root_logger.getEffectiveLevel()
    new_level_string = ConfigIniParser().get_log_level()
    root_logger.setLevel(new_level_string)
    new_level = root_logger.getEffectiveLevel()
    if new_level != current_level:
        root_logger.info(get_thread_info() + "Changed log level to " + new_level_string + ' (' + str(new_level) + ')')