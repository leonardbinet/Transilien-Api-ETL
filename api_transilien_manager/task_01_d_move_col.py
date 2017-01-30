import os
import logging
from os import sys, path
import datetime

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    from api_transilien_manager.utils_misc import set_logging_conf
    set_logging_conf(log_name="task_01_d_move_col.log")

from api_transilien_manager.utils_mongo import mongo_move_day_data_to_other_col
from api_transilien_manager.utils_misc import get_paris_local_datetime_now

logger = logging.getLogger(__name__)

today_paris = get_paris_local_datetime_now()
yesterday_paris = today_paris - datetime.timedelta(days=1)
yesterday_paris_str = yesterday_paris.strftime("%Y%m%d")

logger.info(
    "Begin operation: moving yesterday data from collection real_departures to real_departures_store.")
mongo_move_day_data_to_other_col(
    yesterday_paris_str, "real_departures", "real_departures_store", "request_day", del_original=True)
