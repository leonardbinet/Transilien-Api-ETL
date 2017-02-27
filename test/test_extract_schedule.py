from os import sys, path
import unittest
import logging

from api_etl.extract_schedule import build_trips_ext_df, build_stop_times_ext_df, save_trips_extended_rdb, save_stop_times_extended_rdb

logger = logging.getLogger(__name__)


class TestExtractScheduleModuleFunctions(unittest.TestCase):

    def test_build_trips_ext_df(self):
        """
        Test build_trips_ext function that should return a dataframe:
        - that contains thousands of rows
        - that should contain some necessary fields
        - that should have no NaN value

        """
        trips_ext_df = build_trips_ext_df()

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

    def build_stop_times_ext_df(self):
        """
        Test build_trips_ext function that should return a dataframe:
        - that contains thousands of rows
        - that should contain some necessary fields
        - that should have no NaN value
        """
        stop_times_df = build_stop_times_ext_df()

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
