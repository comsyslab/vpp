import datetime
import unittest

import tzlocal

from vpp.database.schema_manager import SchemaManager


class SchemaManagerTest(unittest.TestCase):

    def test_get_partition_table_name(self):

        schema_manager = SchemaManager(None)

        #Daylight savings time, should be timezone +2
        timestamp = datetime.datetime(year=2015, month=5, day=30, hour=15, minute=25, second=16, microsecond=0, tzinfo=tzlocal.get_localzone())
        table_name = schema_manager.get_partition_table_name(timestamp=timestamp)
        self.assertEqual(table_name, 'Measurement_2015_05_30_00_00_00_02_00')

        #No daylight savings time, should be timezone +1
        timestamp = datetime.datetime(year=2015, month=11, day=30, hour=15, minute=25, second=16, microsecond=0, tzinfo=tzlocal.get_localzone())
        table_name = schema_manager.get_partition_table_name(timestamp=timestamp)
        self.assertEqual(table_name, 'Measurement_2015_11_30_00_00_00_01_00')

        #Just after midnight
        timestamp = datetime.datetime(year=2015, month=5, day=30, hour=0, minute=5, second=0, microsecond=0, tzinfo=tzlocal.get_localzone())
        table_name = schema_manager.get_partition_table_name(timestamp=timestamp)
        self.assertEqual(table_name, 'Measurement_2015_05_30_00_00_00_02_00')

        timestamp = datetime.datetime(year=2015, month=5, day=30, hour=23, minute=59, second=59, microsecond=999, tzinfo=tzlocal.get_localzone())
        table_name = schema_manager.get_partition_table_name(timestamp=timestamp)
        self.assertEqual(table_name, 'Measurement_2015_05_30_00_00_00_02_00')

        timestamp = datetime.datetime(year=2015, month=5, day=31, hour=0, minute=0, second=0, microsecond=0, tzinfo=tzlocal.get_localzone())
        table_name = schema_manager.get_partition_table_name(timestamp=timestamp)
        self.assertEqual(table_name, 'Measurement_2015_05_31_00_00_00_02_00')

    def test_get_partition_table_name_2(self):
        '''Daylight savings time began at
         2014 30. mar 02:00
         and ended at
         2014 26. okt 03:00

        This test verifies that timestamps around the changing times will
        still map to the same single table for that date'''

        schema_manager = SchemaManager(None)

        #Just before entering DST, at 1:59
        timestamp = datetime.datetime(year=2014, month=3, day=30, hour=1, minute=59, second=59, microsecond=0, tzinfo=tzlocal.get_localzone())
        table_name = schema_manager.get_partition_table_name(timestamp=timestamp)
        #print table_name
        self.assertEqual(table_name, 'Measurement_2014_03_30_00_00_00_01_00')

        #Just after entering DST, at 2:01
        timestamp = datetime.datetime(year=2014, month=3, day=30, hour=2, minute=1, second=0, microsecond=0, tzinfo=tzlocal.get_localzone())
        table_name = schema_manager.get_partition_table_name(timestamp=timestamp)
        self.assertEqual(table_name, 'Measurement_2014_03_30_00_00_00_01_00')
        #print table_name

        #Just after entering DST, at 3:01
        timestamp = datetime.datetime(year=2014, month=3, day=30, hour=3, minute=1, second=0, microsecond=0, tzinfo=tzlocal.get_localzone())
        table_name = schema_manager.get_partition_table_name(timestamp=timestamp)
        self.assertEqual(table_name, 'Measurement_2014_03_30_00_00_00_01_00')
        #print table_name

        #Just before leaving DST, at 2:59
        timestamp = datetime.datetime(year=2014, month=10, day=26, hour=2, minute=59, second=0, microsecond=0, tzinfo=tzlocal.get_localzone())
        table_name = schema_manager.get_partition_table_name(timestamp=timestamp)
        self.assertEqual(table_name, 'Measurement_2014_10_26_00_00_00_02_00')

        #Just after leaving DST, at 3:01
        timestamp = datetime.datetime(year=2014, month=10, day=26, hour=3, minute=1, second=0, microsecond=0, tzinfo=tzlocal.get_localzone())
        table_name = schema_manager.get_partition_table_name(timestamp=timestamp)
        self.assertEqual(table_name, 'Measurement_2014_10_26_00_00_00_02_00')