import unittest
import logging

from api_etl.utils_misc import compute_delay

logger = logging.getLogger(__name__)


class TestUtilsMiscFunctions(unittest.TestCase):

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
