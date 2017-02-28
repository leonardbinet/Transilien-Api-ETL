import unittest
import logging

from api_etl.utils_misc import compute_delay, get_station_ids, api_date_to_day_time_corrected

logger = logging.getLogger(__name__)


class TestUtilsMiscFunctions(unittest.TestCase):

    def test_get_station_ids(self):
        """
        Test function that is supposed to return station ids.
        """
        for filt in ["all", "responding", "scheduled", "top"]:
            stations = get_station_ids(stations=filt)
            self.assertIsInstance(stations, list)
            self.assertIsInstance(stations[0], str)
            self.assertEqual(len(stations[0]), 7)
            self.assertGreater(len(stations), 10)

        self.assertRaises(ValueError, get_station_ids, stations="doesnt exist")

    def test_api_date_to_day_time_corrected(self):
        """
        Test function that transform dates given by api in usable fields:
        - expected_passage_day : "20120523" format
        - expected_passage_time: "12:55:00" format

        Important: dates between 0 and 3 AM are transformed in +24h time format with day as previous day.
        """

        api_date_1 = "23/05/2012 12:55"
        day_result_1 = api_date_to_day_time_corrected(api_date_1, "day")
        time_result_1 = api_date_to_day_time_corrected(api_date_1, "time")
        self.assertEqual(day_result_1, "20120523")
        self.assertEqual(time_result_1, "12:55:00")

        api_date_2 = "01/07/2016 01:32"
        day_result_2 = api_date_to_day_time_corrected(api_date_2, "day")
        time_result_2 = api_date_to_day_time_corrected(api_date_2, "time")
        self.assertEqual(day_result_2, "20160630")
        self.assertEqual(time_result_2, "25:32:00")

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


if __name__ == '__main__':
    unittest.main()
