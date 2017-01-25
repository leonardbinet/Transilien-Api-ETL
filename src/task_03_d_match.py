import os
import logging
from os import sys, path
import pytz
import datetime


if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    from src.utils_misc import set_logging_conf
    set_logging_conf(log_name="task_03_d_match.log")

from src.settings import BASE_DIR
from src.mod_03_match_collections import update_real_departures_mongo

logger = logging.getLogger(__name__)

# This operation is done every week
logger.info("Task: daily update: adds trip_id, scheduled_departure_time, delay")

paris_tz = pytz.timezone('Europe/Paris')
today_paris = paris_tz.localize(datetime.datetime.now())
today_paris_str = today_paris.strftime("%Y%m%d")
yesterday_paris = today_paris - datetime.timedelta(days=1)
yesterday_paris_str = yesterday_paris.strftime("%Y%m%d")

update_real_departures_mongo(yesterday_paris, threads=5)
