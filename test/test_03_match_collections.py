from os import sys, path
import unittest
import pytz
import datetime
import logging

from src.settings import BASE_DIR
from src.mod_03_match_collections import compute_delay, api_train_num_to_trip_id, update_real_departures_mongo
from src.utils_misc import get_paris_local_datetime_now

logger = logging.getLogger(__name__)


class TestMatchingModuleFunctions(unittest.TestCase):

    def test_compute_delay(self):
        # real_departure_date = "01/02/2017 22:12" (api format)
        # scheduled_departure_time = '22:12:00' (schedules format)
        # scheduled_departure_day = '20170102' (schedules format)
        # We don't need to take into account time zones
        sch_dep_time = '22:12:00'
        real_dep_time = "22:12:00"
        self.assertEqual(compute_delay(sch_dep_time, real_dep_time), 0)
        sch_dep_time = '22:12:00'
        real_dep_time = "22:11:00"
        self.assertEqual(compute_delay(sch_dep_time, real_dep_time), -60)
        sch_dep_time = '22:12:00'
        real_dep_time = "22:13:00"
        self.assertEqual(compute_delay(sch_dep_time, real_dep_time), 60)
        sch_dep_time = '23:59:00'
        real_dep_time = "00:01:00"
        self.assertEqual(compute_delay(sch_dep_time, real_dep_time), 120)
        sch_dep_time = '00:01:00'
        real_dep_time = "23:59:00"
        self.assertEqual(compute_delay(sch_dep_time, real_dep_time), -120)

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
