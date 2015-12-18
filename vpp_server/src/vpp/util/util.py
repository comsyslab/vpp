import multiprocessing
import threading

import datetime


def get_fully_qualified_name(cls):
    return cls.__module__ + "." + cls.__name__

def get_thread_info():
    return multiprocessing.current_process().name + " " + threading.currentThread().name + " "

def secs_to_ms(secs):
    return int(secs*1000)

def init_strptime():
    datetime.datetime.strptime('', '')
