import os
import logging
from os import sys, path
from src.mod_01_extract import operate_timer

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))

logging_file_path = os.path.join(BASE_DIR, "..", "logs", "command_line.log")
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                    filename=logging_file_path, level=logging.INFO)


def execute_from_command_line(args):
    # print("Base directory is: ", settings.BASE_DIR)
    logging.debug("Arguments passed: %s" % args)
    if len(args) == 1:
        logging.info("No argument passed.. nothing.")
        return False

    if args[1] == "extract":
        stop_time = 300000
        if len(args) > 2:
            try:
                cycle_time_sec = int(args[2])
                logging.info("Launch extraction with cycle time of %d seconds" %
                             cycle_time_sec)
            except:
                logging.warn(
                    "Time in seconds must be an integer. 2nd arg not taken into account.")
        else:
            logging.info("Launch extraction with cycle time of 1200 seconds")
            cycle_time_sec = 1200
            operate_timer(cycle_time_sec=cycle_time_sec,
                          stop_time_sec=stop_time)
