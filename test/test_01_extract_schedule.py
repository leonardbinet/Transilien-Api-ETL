from os import sys, path
import unittest
import pytz
import datetime
import logging

from src.settings import BASE_DIR
from src.mod_01_extract_schedule import write_flat_departures_times_df

logger = logging.getLogger(__name__)


class TestExtractScheduleModuleFunctions(unittest.TestCase):

    pass


if __name__ == '__main__':
    unittest.main()
