from os import sys, path
import unittest
import pytz
import datetime

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.settings import BASE_DIR
from src.mod_02_find_schedule import get_departure_times_of_day_json_list


class TestSchedulesModuleFunctions(unittest.TestCase):

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
