import datetime
from unittest import TestCase

from vpp.data_acquisition.adapter.adapter_date_strategy import DefaultAdapterFileDateStrategy


class DefaultDateStrategyTest(TestCase):

    def test_get_file_date_1(self):
        regex = 'delta-571313115104.{6}-(2016)-([0-1][0-9])-([0-3][0-9])\.xml'
        self.check_file_name('delta-571313115104261472-2016-01-21.xml', regex, 2016, 1, 21, 0)

    def test_get_file_date_2(self):
        regex = 'steffen_w([0-9]{4})([0-1][0-9])([0-3][0-9])([0-2][0-9])dat'
        self.check_file_name('steffen_w2014110700dat', regex, 2014, 11, 7, 0)

    def check_file_name(self, file_name, regex, expected_year, expected_month, expected_day, expected_hour):
        date_helper = DefaultAdapterFileDateStrategy(FTPConfig(file_pattern=regex))
        date = date_helper.get_date_from_filename(file_name)
        self.assertEqual(date.year, expected_year)
        self.assertEqual(date.month, expected_month)
        self.assertEqual(date.day, expected_day)
        self.assertEqual(date.hour, expected_hour)

    def test_file_already_processed(self):
        regex = 'delta-(2014)-([0-1][0-9])-([0-3][0-9])\.xml'
        date_helper = DefaultAdapterFileDateStrategy(FTPConfig(file_pattern=regex))
        file_name_1 = 'delta-2014-03-02.xml'
        file_name_2 = 'delta-2014-03-03.xml'

        self.assertFalse(date_helper.should_fetch_file(file_name_1))
        self.assertTrue(date_helper.should_fetch_file(file_name_2))

    def test_date_already_processed(self):
        date_helper = DefaultAdapterFileDateStrategy(FTPConfig())
        date_1 = datetime.datetime(2014, 3, 2)
        date_2 = datetime.datetime(2014, 3, 3)
        self.assertFalse(date_helper.should_process_date(date_1))
        self.assertTrue(date_helper.should_process_date(date_2))

    def test_date_already_processed_2(self):
        date_helper = DefaultAdapterFileDateStrategy(FTPConfig(fetch_again_hours=24, fetch_again_when_date_equal=True))

        date_0 = datetime.datetime(2014, 2, 28)
        self.assertFalse(date_helper.should_process_date(date_0))

        date_1 = datetime.datetime(2014, 3, 1)
        self.assertTrue(date_helper.should_process_date(date_1))

        date_2 = datetime.datetime(2014, 3, 2)
        self.assertTrue(date_helper.should_process_date(date_2))

        date_3 = datetime.datetime(2014, 3, 3)
        self.assertTrue(date_helper.should_process_date(date_3))

    def test_get_last_fetch_date_from_config_1(self):
        date_helper = DefaultAdapterFileDateStrategy(FTPConfig())
        date = datetime.datetime(2014, 3, 2)
        self.assertEqual(date_helper._get_last_fetch_date_from_config(), date)

    def test_get_last_fetch_date_from_config_2(self):
        ftp_config = FTPConfig()
        ftp_config.last_fetch_adapter = None
        date_helper = DefaultAdapterFileDateStrategy(ftp_config)
        date = datetime.datetime(1970, 1, 1)
        self.assertEqual(date_helper._get_last_fetch_date_from_config(), date)

    def test_update_newest_filename(self):
        regex = 'delta-(2016)-([0-1][0-9])-([0-3][0-9])\.xml'
        ftp_config = FTPConfig(file_pattern=regex)
        date_helper = DefaultAdapterFileDateStrategy(ftp_config)
        file_name = 'delta-2016-07-28.xml'

        date_helper.update_latest_fetch_for_file(file_name)
        expected_date = datetime.datetime(2016, 7, 28)
        self.assertEqual(ftp_config.last_fetch_adapter, expected_date.isoformat())

    def test_update_newest_file(self):
        ftp_config = FTPConfig()
        date_helper = DefaultAdapterFileDateStrategy(ftp_config)
        new_date = datetime.datetime(2016, 2, 25)
        date_helper.update_latest_fetch_date(new_date)
        self.assertEqual(ftp_config.last_fetch_adapter, new_date.isoformat())


class FTPConfig(object):
    def __init__(self, file_pattern='', fetch_again_hours=0, fetch_again_when_date_equal=False):
        self.last_fetch_adapter = '2014-03-02T00:00:00'
        self.file_pattern = file_pattern
        self.fetch_again_hours = fetch_again_hours
        self.fetch_again_when_date_equal = fetch_again_when_date_equal
