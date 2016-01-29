import iso8601


class DefaultInterpreterDateStrategy(object):

    def __init__(self, ftp_config):
        self.ftp_config = ftp_config

    def should_process_date(self, date):
        return date > self._get_fetch_date_from_config()

    def update_latest_fetch_date(self, new_date):
        if new_date is None:
            return

        if new_date <= self._get_fetch_date_from_config():
            return

        self.ftp_config.last_fetch_interpreter = new_date.isoformat()

    def _get_fetch_date_from_config(self):
        if not self.ftp_config.last_fetch_interpreter:
            return self.base_date
        return iso8601.parse_date(self.ftp_config.last_fetch_interpreter, default_timezone=None)