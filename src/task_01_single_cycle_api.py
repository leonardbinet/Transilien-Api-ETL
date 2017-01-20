import os
import logging
from os import sys, path

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    logging_file_path = os.path.join(
        BASE_DIR, "..", "logs", "task_01_single_cycle.log")
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                        filename=logging_file_path, level=logging.INFO)

from src.mod_01_extract import operate_one_cycle

# Default: all stations, and max 300 queries per sec
operate_one_cycle(station_filter=False, max_per_minute=300)
