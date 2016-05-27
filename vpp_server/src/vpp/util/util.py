import logging
import multiprocessing
import os
import threading

import datetime

from vpp.config.config_ini_parser import ConfigIniParser

sem = threading.Semaphore()
con_count = 0

def log_open_db_connection():
    global con_count
    global sem
    sem.acquire()
    con_count += 1
    string = get_thread_info() + "Opened DB connection. " + str(con_count) + " connections open"
    sem.release()
    logger = logging.getLogger(__name__)
    logger.debug(string)
    if con_count > 2:
        logger.warning(string)
    else:
        logger.info(string)

def log_close_db_connection():
    global con_count
    global sem
    sem.acquire()
    con_count -= 1
    sem.release()
    logger = logging.getLogger(__name__)
    string = get_thread_info() + "Closed DB connection. " + str(con_count) + " connections open"

    if con_count >= 1:
        logger.warning(string)
    else:
        logger.info(string)


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


def get_base_date(tzinfo=None):
    return datetime.datetime(1970, 1, 1, tzinfo=tzinfo)


def get_data_provider_ini_files():
    data_provider_config_dir = '../resources/data_providers'
    files = []
    for file_name in os.listdir(data_provider_config_dir):
        rel_file_path = data_provider_config_dir + os.sep + file_name
        if os.path.isfile(rel_file_path) and rel_file_path.endswith('.ini'):
            files.append(rel_file_path)

    return files
