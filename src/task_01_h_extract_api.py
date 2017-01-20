import os
import logging
from os import sys, path

from settings import BASE_DIR

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    logging_file_path = os.path.join(BASE_DIR, "..", "logs", "task01.log")
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                        filename=logging_file_path, level=logging.INFO)

from src.mod_01_extract import operate_multiple_cycles

# By default, run for one hour (minus 100 sec), every 2 minutes
# max 300 queries per sec
operate_multiple_cycles(station_filter=False, cycle_time_sec=120,
                        stop_time_sec=3500, max_per_minute=300)
