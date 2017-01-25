from os import sys, path
import unittest
import pytz
import datetime
import logging

from src.settings import BASE_DIR
from src.mod_03_match_collections import compute_delay, api_train_num_to_trip_id

logger = logging.getLogger(__name__)


class TestMatchingModuleFunctions(unittest.TestCase):

    def test_compute_delay(self):
        # real_departure_date = "01/02/2017 22:12" (api format)
        # scheduled_departure_time = '22:12:00' (schedules format)
        # scheduled_departure_day = '20170102' (schedules format)
        # We don't need to take into account time zones
        sch_dep_time = '22:12:00'
        real_dep_date = "01/02/2017 22:12"
        self.assertEqual(compute_delay(sch_dep_time, real_dep_date), 0)
        sch_dep_time = '22:10:00'
        real_dep_date = "01/02/2017 22:12"
        self.assertEqual(compute_delay(sch_dep_time, real_dep_date), 120)

    def test_api_train_num_to_trip_id(self):
        train_num = "110313"
        trip_id = "DUASN110313F03001-1_419961"
        start_date = "20170102"
        end_date = "20170707"
        day = "20170202"
        weekday = "thursday"
        found_trip_id = api_train_num_to_trip_id(train_num, day, weekday)
        self.assertEqual(found_trip_id, trip_id)


if __name__ == '__main__':
    unittest.main()
