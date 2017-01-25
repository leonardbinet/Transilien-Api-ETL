import os
import logging
from os import sys, path

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    from src.utils_misc import set_logging_conf
    set_logging_conf(log_name="task02_w_dl_gtfs.log")

from src.mod_01_extract_schedule import download_gtfs_files, write_flat_departures_times_df, save_all_schedule_tables_rdb

logger = logging.getLogger(__name__)

# This operation is done every week
logger.info("Task: weekly update of gtfs files")

logger.info("Download files.")
download_gtfs_files()

logging.info("Create flat csv.")
write_flat_departures_times_df()

logging.info("Inserting data in postgres")
save_all_schedule_tables_rdb()
