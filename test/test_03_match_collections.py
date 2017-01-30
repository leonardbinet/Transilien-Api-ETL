from os import sys, path
import unittest
import pytz
import datetime
import logging

from api_transilien_manager.settings import BASE_DIR
from api_transilien_manager.mod_03_match_collections import api_train_num_to_trip_id, update_real_departures_mongo
from api_transilien_manager.utils_misc import get_paris_local_datetime_now

logger = logging.getLogger(__name__)


class TestMatchingModuleFunctions(unittest.TestCase):

    def test_api_train_num_to_trip_id(self):
        train_num = "110313"
        trip_id = "DUASN110313F03001-1_419961"
        start_date = "20170102"
        end_date = "20170707"
        day = "20170202"
        weekday = "thursday"
        found_trip_id = api_train_num_to_trip_id(train_num, day, weekday)
        self.assertEqual(found_trip_id, trip_id)

    def test_update_real_departures_mongo(self):
        today_paris = get_paris_local_datetime_now()
        today_paris_str = today_paris.strftime("%Y%m%d")
        yesterday_paris = today_paris - datetime.timedelta(days=1)
        yesterday_paris_str = yesterday_paris.strftime("%Y%m%d")
        logger.info("Paris yesterday date is %s" % yesterday_paris_str)

        update_real_departures_mongo(
            str(today_paris_str), threads=5, limit=100)


if __name__ == '__main__':
    unittest.main()
