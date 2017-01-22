from os import sys, path
import unittest
import pytz
import datetime
import logging

from src.settings import BASE_DIR
from src.mod_02_find_schedule import get_departure_times_of_day_json_list, trip_scheduled_departure_time

logger = logging.getLogger(__name__)


class TestSchedulesModuleFunctions(unittest.TestCase):

    def test_trip_scheduled_departures_time(self):
        # Takes to arguments: trip_id, station
        # Must accept both 7 and 8 digits stations
        # trip_id: DUASN124705F01001-1_408049
        # station: 8727605 // 8727605*
        # awaited result: departure_time 04:06:00
        trip_id = "DUASN124705F01001-1_408049"
        station_id = "8727605"
        result1 = trip_scheduled_departure_time(trip_id, station_id)
        self.assertEqual(result1, "04:06:00")
        # Must return False if nothing found
        result2 = trip_scheduled_departure_time("false_trip_id", station_id)
        self.assertFalse(result2)
        result3 = trip_scheduled_departure_time(trip_id, "false_station_id")
        self.assertFalse(result3)

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


if __name__ == '__main__':
    unittest.main()
