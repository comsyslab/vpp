import logging


class QueueHandler(logging.Handler):
    """
    This is a logging handler which sends events to a multiprocessing queue.
    """

    def __init__(self, queue):
        """
        Initialise an instance, using the passed queue.
        """
        logging.Handler.__init__(self)
        self.queue = queue

    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the queue.
        """

        try:
            self.decorate_record(record)
            self.queue.put_nowait(record)

        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def decorate_record(self, record):
        if not record.exc_info:
            return

        try:
            dummy = self.format(record)  # just to get traceback text into record.exc_text
        except:
            pass

        record.exc_info = None  # not needed any more
