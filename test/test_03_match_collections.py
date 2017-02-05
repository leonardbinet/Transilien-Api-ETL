from os import sys, path
import unittest
import pytz
import datetime
import logging
import pandas as pd
import random

from api_transilien_manager.settings import BASE_DIR
from api_transilien_manager.mod_01_extract_schedule import build_trips_ext_df
from api_transilien_manager.mod_03_match_collections import api_train_num_to_trip_id, update_real_departures_mongo
from api_transilien_manager.utils_misc import get_paris_local_datetime_now

logger = logging.getLogger(__name__)


class TestMatchingModuleFunctions(unittest.TestCase):

    def test_api_train_num_to_trip_id(self):
        """
        Check that rdb answers return what is really written in gtfs files:
        - compute trips_ext dataframe
        - check for each element if it scheduled on this day
        - get for each element rdb answer
        - compare
        - count number of errors
        """
        # day = "20170202"
        # Take today's date
        day = datetime.datetime.now().strftime("%Y%m%d")

        def is_scheduled_on_day(day, start, end):
            start_cond = day >= start
            end_cond = start <= end
            return start_cond and end_cond

        test_trips_df = build_trips_ext_df()

        random_indexes = random.sample(range(0, len(test_trips_df) - 1), 10)
        test_trips_df = test_trips_df.iloc[random_indexes]
        # logger.info(test_trips_df)

        test_trips_df["scheduled"] = test_trips_df.apply(
            lambda x: is_scheduled_on_day(int(day), x["start_date"], x["end_date"]), axis=1)

        # get rdb answers
        test_trips_df["rdb_answer"] = test_trips_df.apply(
            lambda x: api_train_num_to_trip_id(x["train_num"], day), axis=1)

        # match when answer == (trip_id or False)/ (trip_id or True)
        test_trips_df["match"] = test_trips_df.apply(
            lambda x: x["rdb_answer"] == (x["trip_id"] or x["scheduled"]), axis=1)

        nb_false = test_trips_df[test_trips_df.match != True].match.sum()
        self.assertEqual(nb_false, 0)

    def test_update_real_departures_mongo(self):
        """
        For now, only launch real update process and checks if no error occurs.
        """
        today_paris = get_paris_local_datetime_now()
        today_paris_str = today_paris.strftime("%Y%m%d")
        yesterday_paris = today_paris - datetime.timedelta(days=1)
        yesterday_paris_str = yesterday_paris.strftime("%Y%m%d")
        logger.info("Paris yesterday date is %s" % yesterday_paris_str)

        update_real_departures_mongo(
            str(today_paris_str), threads=5, limit=10)


if __name__ == '__main__':
    unittest.main()
