import datetime
from unittest import TestCase

from vpp.data_acquisition.adapter.file_date_helper import FileDateHelper


class FileDateHelperTest(TestCase):

    def test_get_file_date_1(self):
        regex = 'delta-571313115104.{6}-(2016)-([0-1][0-9])-([0-3][0-9])\.xml'
        self.check_file_name('delta-571313115104261472-2016-01-21.xml', regex, 2016, 1, 21, 0)

    def test_get_file_date_2(self):
        regex = 'steffen_w([0-9]{4})([0-1][0-9])([0-3][0-9])([0-2][0-9])dat'
        self.check_file_name('steffen_w2014110700dat', regex, 2014, 11, 7, 0)

    def check_file_name(self, file_name, regex, expected_year, expected_month, expected_day, expected_hour):
        date_helper = FileDateHelper(None)
        date = date_helper.get_file_date(file_name)
        self.assertEqual(date.year, expected_year)
        self.assertEqual(date.month, expected_month)
        self.assertEqual(date.day, expected_day)
        self.assertEqual(date.hour, expected_hour)

    def test_file_already_processed(self):
        date_helper = FileDateHelper(FTPConfig())
        file_name_1 = 'delta-2014-03-02.xml'
        file_name_2 = 'delta-2014-03-03.xml'
        regex = 'delta-(2014)-([0-1][0-9])-([0-3][0-9])\.xml'

        self.assertTrue(date_helper.file_already_processed(file_name_1))
        self.assertFalse(date_helper.file_already_processed(file_name_2))

    def test_date_already_processed(self):
        date_helper = FileDateHelper(FTPConfig())
        date_1 = datetime.datetime(2014, 3, 2)
        date_2 = datetime.datetime(2014, 3, 3)
        self.assertTrue(date_helper.date_already_processed(date_1))
        self.assertFalse(date_helper.date_already_processed(date_2))

    def test_get_last_fetch_date_from_config_1(self):
        date_helper = FileDateHelper(FTPConfig())
        date = datetime.datetime(2014, 3, 2)
        self.assertEqual(date_helper._get_last_fetch_date_from_config(), date)

    def test_get_last_fetch_date_from_config_2(self):
        ftp_config = FTPConfig()
        ftp_config.last_fetch = None
        date_helper = FileDateHelper(ftp_config)
        date = datetime.datetime(1970, 1, 1)
        self.assertEqual(date_helper._get_last_fetch_date_from_config(), date)

    def test_update_newest_filename(self):
        ftp_config = FTPConfig()
        date_helper = FileDateHelper(ftp_config)
        file_name = 'delta-2016-07-28.xml'
        regex = 'delta-(2016)-([0-1][0-9])-([0-3][0-9])\.xml'
        date_helper.update_latest_fetch_for_file(file_name)
        expected_date = datetime.datetime(2016, 7, 28)
        self.assertEqual(ftp_config.last_fetch, expected_date.isoformat())

    def test_update_newest_file(self):
        ftp_config = FTPConfig()
        date_helper = FileDateHelper(ftp_config)
        new_date = datetime.datetime(2016, 2, 25)
        date_helper.update_latest_fetch_date(new_date)
        self.assertEqual(ftp_config.last_fetch, new_date.isoformat())


class FTPConfig(object):
    def __init__(self):
        self.last_fetch = '2014-03-02T00:00:00'
