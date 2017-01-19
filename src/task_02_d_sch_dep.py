import os
import logging
from os import sys, path
import datetime
import pytz

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    logging_file_path = os.path.join(
        BASE_DIR, "..", "logs", "task02_d_sch_dep.log")
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                        filename=logging_file_path, level=logging.INFO)

from src.utils_mongo import mongo_async_upsert_chunks
from src.mod_02_find_schedule import save_scheduled_departures_of_day_mongo

# Save for this day, and for next day
paris_tz = pytz.timezone('Europe/Paris')

today_paris = paris_tz.localize(datetime.date.today())
today_paris_str = today_paris.strftime("%Y%m%d")
tomorrow_paris = today_paris + datetime.timedelta(days=1)
tomorrow_paris_str = tomorrow_paris.strftime("%Y%m%d")

save_scheduled_departures_of_day_mongo(today_paris_str)
save_scheduled_departures_of_day_mongo(tomorrow_paris_str)
