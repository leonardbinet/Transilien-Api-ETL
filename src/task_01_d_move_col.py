import os
import logging
from os import sys, path
import datetime
import pytz

from settings import BASE_DIR

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    from src.utils_misc import set_logging_conf
    set_logging_conf(log_name="task_01_d_move_col.log")


from src.utils_mongo import mongo_move_day_data_to_other_col

logger = logging.getLogger(__name__)

paris_tz = pytz.timezone('Europe/Paris')

today_paris = paris_tz.localize(datetime.datetime.now())
yesterday_paris = today_paris - datetime.timedelta(days=1)
yesterday_paris_str = yesterday_paris.strftime("%Y%m%d")

logger.info(
    "Begin operation: moving yesterday data from collection real_departures to real_departures_store.")
mongo_move_day_data_to_other_col(
    yesterday_paris_str, "real_departures", "real_departures_store", "request_day", del_original=True)
