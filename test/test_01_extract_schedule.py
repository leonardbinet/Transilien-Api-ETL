from os import sys, path
import unittest
import pytz
import datetime
import logging

from api_transilien_manager.settings import BASE_DIR
from api_transilien_manager.mod_01_extract_schedule import write_flat_departures_times_df, save_trips_extended_rdb, save_stop_times_extended_rdb

logger = logging.getLogger(__name__)


class TestExtractScheduleModuleFunctions(unittest.TestCase):

    def test_save_trips_extended_rdb(self):
        trips_ext_df = save_trips_extended_rdb(dryrun=True)

        # check is necessary fields are present (all except block_id)
        necessary_fields = [
            'route_id', 'service_id', 'trip_id', 'trip_headsign',
            'direction_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date',
            'train_num'
        ]
        df_cols = trips_ext_df.columns.values
        keys_all_exist = all(key in df_cols for key in necessary_fields)
        self.assertTrue(keys_all_exist)

        # check is no null value
        nb_null = trips_ext_df.isnull().sum().sum()
        self.assertEqual(nb_null, 0)

        # check if not empty
        self.assertGreater(len(trips_ext_df), 1000)

    def test_save_stop_times_extended_rdb(self):
        stop_times_df = save_stop_times_extended_rdb(dryrun=True)

        # check is necessary fields are present (all except block_id)
        necessary_fields = [
            'trip_id', 'arrival_time', 'departure_time', 'stop_id',
            'stop_sequence', 'pickup_type', 'drop_off_type', 'route_id',
            'service_id', 'trip_headsign', 'direction_id', 'monday', 'tuesday',
            'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
            'start_date', 'end_date', 'stop_name', 'stop_lat', 'stop_lon',
            'location_type', 'parent_station'
        ]

        df_cols = stop_times_df.columns.values
        keys_all_exist = all(key in df_cols for key in necessary_fields)
        self.assertTrue(keys_all_exist)

        # check is no null value
        nb_null = stop_times_df.isnull().sum().sum()
        self.assertEqual(nb_null, 0)

        # check if not empty
        self.assertGreater(len(stop_times_df), 10000)


if __name__ == '__main__':
    unittest.main()
