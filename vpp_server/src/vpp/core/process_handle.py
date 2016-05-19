import multiprocessing
from multiprocessing import Queue, Process
import threading



from vpp.core.abstract_process import AbstractVPPProcess
from vpp.util import util

__author__ = 'ubbe'

import logging

class ProcessHandle(object):

    def __init__(self, process_class):
        self.logger = logging.getLogger(__name__)
        self.process_class = process_class

    def start(self):
        self.out_queue = Queue()
        self.log_queue = Queue()
        self._run_log_queue_thread()
        self.process = Process(name=self.process_class.__name__, target=self.process_class, args=(self.out_queue, self.log_queue))
        self.process.start()

    def _run_log_queue_thread(self):
        self.log_thread = threading.Thread(target=self._listen_on_log_queue)
        self.log_thread.setDaemon(False)
        self.log_thread.start()

    def _listen_on_log_queue(self):
        record = self.log_queue.get()
        if record.msg.count('501') > 0:
            pass
        while record != 'STOP':
            try:
                logger = logging.getLogger(record.name)
                if record.levelno >= logger.getEffectiveLevel():
                    logger.handle(record)
                record = self.log_queue.get()
            except IOError:
                break
            except Exception as e:
                self.logger.exception(e.message)
        self.logger.debug(util.get_thread_info() + ' Log queue listener thread exited.')

    def stop(self):
        self.out_queue.put(AbstractVPPProcess.Commands.STOP)
        self.logger.debug(util.get_thread_info() + "ProcessManager sent stop to " + self.process.name)
        self.out_queue.close()

        thread = threading.Thread(target=self._kill_after_timeout)
        thread.setDaemon(False)
        thread.start()

    def _kill_after_timeout(self):
        timeout = 15
        self.logger.debug(util.get_thread_info() + "Waiting for process " + self.process.name + " to exit (timeout " + str(timeout) + ")")
        self.process.join(timeout=timeout)

        if (self.process.is_alive()):
            self.process.terminate()
            self.logger.info(util.get_thread_info() + "Timeout exceeded, forcefully terminating process " + self.process.name)
        else:
            self.logger.info(util.get_thread_info() + self.process.name + " exited gracefully")
        self.log_queue.put('STOP')
        self.log_queue.close()

    def join(self):
        self.logger.debug(util.get_thread_info() + "Joining process " + self.process.name)
        self.process.join()
        self.logger.debug(util.get_thread_info() + "Joined " + self.process.name)

