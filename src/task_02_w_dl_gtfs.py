import os
import logging
from os import sys, path

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    logging_file_path = os.path.join(
        BASE_DIR, "..", "logs", "task02_w_dl_gtfs.log")
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                        filename=logging_file_path, level=logging.INFO)

from src.mod_02_find_schedule import download_gtfs_files

# This operation is done every week

download_gtfs_files()
