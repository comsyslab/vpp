import datetime
import unittest

import pytz
import tzlocal

from vpp.database.schema_manager import SchemaManager


class SchemaManagerTest(unittest.TestCase):

    def test_get_partition_table_name_1(self):

        schema_manager = SchemaManager(None, None)
        timezone_cph = pytz.timezone('Europe/Copenhagen')

        timestamp = timezone_cph.localize(datetime.datetime(year=2015, month=4, day=7, hour=0, minute=5, second=0, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        self.assertEqual('Measurement_2015_04_06_00_00_00', table_name)

    def test_get_partition_table_name_2(self):

        schema_manager = SchemaManager(None, None)
        timezone_cph = pytz.timezone('Europe/Copenhagen')

        #Daylight savings time, should be timezone +2
        timestamp = timezone_cph.localize(datetime.datetime(year=2015, month=5, day=30, hour=15, minute=25, second=16, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        self.assertEqual('Measurement_2015_05_30_00_00_00', table_name)

        #No daylight savings time, should be timezone +1
        timestamp = timezone_cph.localize(datetime.datetime(year=2015, month=11, day=30, hour=15, minute=25, second=16, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        self.assertEqual('Measurement_2015_11_30_00_00_00', table_name)

        #Just after midnight CET is actually before midnight UTC
        timestamp = timezone_cph.localize(datetime.datetime(year=2015, month=5, day=30, hour=0, minute=5, second=0, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        self.assertEqual('Measurement_2015_05_29_00_00_00', table_name)

        timestamp = timezone_cph.localize(datetime.datetime(year=2015, month=5, day=30, hour=23, minute=59, second=59, microsecond=999))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        self.assertEqual('Measurement_2015_05_30_00_00_00', table_name)

        #Just after midnight CET is actually before midnight UTC
        timestamp = timezone_cph.localize(datetime.datetime(year=2015, month=5, day=31, hour=0, minute=0, second=0, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        self.assertEqual('Measurement_2015_05_30_00_00_00', table_name)

    def test_get_partition_table_name_3(self):
        '''Daylight savings time began at
         2014 30. mar 02:00
         and ended at
         2014 26. okt 03:00

        This test verifies that timestamps around the changing times will
        still map to the same single table for that date'''

        schema_manager = SchemaManager(None, None)
        timezone_cph = pytz.timezone('Europe/Copenhagen')

        #Just before entering DST, at 1:59
        timestamp = timezone_cph.localize(datetime.datetime(year=2014, month=3, day=30, hour=1, minute=59, second=59, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        #print table_name
        self.assertEqual('Measurement_2014_03_30_00_00_00', table_name)

        #Just after entering DST, at 2:01
        timestamp = timezone_cph.localize(datetime.datetime(year=2014, month=3, day=30, hour=2, minute=1, second=0, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        self.assertEqual('Measurement_2014_03_30_00_00_00', table_name)
        #print table_name

        #Just after entering DST, at 3:01
        timestamp = timezone_cph.localize(datetime.datetime(year=2014, month=3, day=30, hour=3, minute=1, second=0, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        self.assertEqual('Measurement_2014_03_30_00_00_00', table_name)
        #print table_name

        #Just before leaving DST, at 2:59
        timestamp = timezone_cph.localize(datetime.datetime(year=2014, month=10, day=26, hour=2, minute=59, second=0, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        self.assertEqual('Measurement_2014_10_26_00_00_00', table_name)

        #Just after leaving DST, at 3:01
        timestamp = timezone_cph.localize(datetime.datetime(year=2014, month=10, day=26, hour=3, minute=1, second=0, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        self.assertEqual('Measurement_2014_10_26_00_00_00', table_name)


    def test_get_partition_table_name_4(self):
        schema_manager = SchemaManager(None, None)
        timezone_cph = pytz.timezone('Europe/Copenhagen')

        #2014-08-19 01:59 CET -> 2014-08-18 23:59 UTC
        timestamp = timezone_cph.localize(datetime.datetime(year=2014, month=8, day=19, hour=1, minute=59, second=0, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        #print table_name
        self.assertEqual(table_name, 'Measurement_2014_08_18_00_00_00')

        #2014-08-19 02:01 CET -> 2014-08-19 00:01 UTC
        timestamp = timezone_cph.localize(datetime.datetime(year=2014, month=8, day=19, hour=2, minute=01, second=0, microsecond=0))
        table_name = schema_manager.get_partition_subtable_name(schema_manager.measurement_base_table_name, timestamp=timestamp)
        #print table_name
        self.assertEqual('Measurement_2014_08_19_00_00_00', table_name)


