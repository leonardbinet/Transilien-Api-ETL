from os import sys, path
import unittest
import pytz
import datetime
import logging
import random

from api_transilien_manager.settings import BASE_DIR
from api_transilien_manager.mod_01_extract_schedule import build_stop_times_ext_df
from api_transilien_manager.mod_02_query_schedule import get_departure_times_of_day_json_list, trip_scheduled_departure_time, rdb_get_departure_times_of_day_json_list

logger = logging.getLogger(__name__)


class TestSchedulesModuleFunctions(unittest.TestCase):

    def test_trip_scheduled_departures_time(self):
        # Takes to arguments: trip_id, station
        # Must accept both 7 and 8 digits stations
        # trip_id: DUASN124705F01001-1_408049
        # station: 8727605 // 8727605*
        # awaited result: departure_time 04:06:00 or False

        # Test scenario:
        # get random example for stop_times_ext dataframe
        # check if we get same results from sql queries and from dataframe

        # take 10 random elements from dataframe
        df = build_stop_times_ext_df()
        random_indexes = random.sample(range(0, len(df) - 1), 10)

        for random_index in random_indexes:
            trip_id = df.iloc[random_index]["trip_id"]
            station_id = df.iloc[random_index]["station_id"]
            departure_time = df.iloc[random_index]["departure_time"]

            result1 = trip_scheduled_departure_time(trip_id, station_id)
            self.assertEqual(result1, departure_time)

            # False trip_id should return False
            result2 = trip_scheduled_departure_time(
                "false_trip_id", station_id)
            self.assertFalse(result2)

            # False station_id should return False
            result3 = trip_scheduled_departure_time(
                trip_id, "false_station_id")
            self.assertFalse(result3)

    """
    # Deprecated

    def test_get_departure_times_of_day_json_list(self):
        necessary_fields = ["scheduled_departure_day",
                            "scheduled_departure_time", "trip_id", "station_id", "train_num"]

        paris_tz = pytz.timezone('Europe/Paris')
        today_paris = paris_tz.localize(datetime.datetime.now())
        today_paris_str = today_paris.strftime("%Y%m%d")

        json_list = get_departure_times_of_day_json_list(today_paris_str)
        json_keys_list = list(map(lambda x: list(x.keys()), json_list))
        for json_item_keys in json_keys_list:
            keys_all_exist = all(
                key in json_item_keys for key in necessary_fields)
            self.assertTrue(keys_all_exist)
    """

    def test_rdb_get_departure_times_of_day_json_list(self):
        paris_tz = pytz.timezone('Europe/Paris')
        today_paris = paris_tz.localize(datetime.datetime.now())
        today_paris_str = today_paris.strftime("%Y%m%d")

        json_list = rdb_get_departure_times_of_day_json_list(
            today_paris_str)

        # Test if all fields are present
        necessary_fields = ["scheduled_departure_day",
                            "scheduled_departure_time", "trip_id", "station_id", "train_num"]
        json_keys_list = list(map(lambda x: list(x.keys()), json_list))
        for json_item_keys in json_keys_list:
            keys_all_exist = all(
                key in json_item_keys for key in necessary_fields)
            self.assertTrue(keys_all_exist)

        # Test if scheduled departure day is really on given day


if __name__ == '__main__':
    unittest.main()
