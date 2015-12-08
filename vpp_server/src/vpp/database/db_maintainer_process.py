import logging

from vpp.database.db_manager import DBManager


class DBMaintainerProcess(object):

    def __init__(self, in_queue):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("DataMaintainerProcess started")
        self.in_queue = in_queue
        self.command = None
        self.db_manager = DBManager()

        self._listen_for_commands()


