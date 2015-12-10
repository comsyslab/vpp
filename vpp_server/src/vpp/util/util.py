import multiprocessing
import threading


def get_fully_qualified_name(cls):
    return cls.__module__ + "." + cls.__name__

def get_thread_info():
    return multiprocessing.current_process().name + " " + threading.currentThread().name + " "