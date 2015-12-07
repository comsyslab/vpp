class DBMaintainerProcess(object):

    def __init__(self, in_queue):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("DataProviderProcess started")
        self.in_queue = in_queue
        self.command = None
        self.db_manager = DBManager()
        self._init_data_providers()
        self._run_data_providers()
        self._listen_for_commands()


