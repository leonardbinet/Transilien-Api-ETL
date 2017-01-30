import os
import logging
from os import sys, path
from api_transilien_manager.mod_01_extract import operate_timer
from api_transilien_manager.settings import BASE_DIR

logger = logging.getLogger(__name__)


def execute_from_command_line(args):
    # print("Base directory is: ", settings.BASE_DIR)
    logger.debug("Arguments passed: %s" % args)
    if len(args) == 1:
        logger.info("No argument passed.. nothing.")
        return False

    if args[1] == "extract":
        stop_time = 300000
        if len(args) > 2:
            try:
                cycle_time_sec = int(args[2])
                logger.info("Launch extraction with cycle time of %d seconds" %
                            cycle_time_sec)
            except:
                logger.warn(
                    "Time in seconds must be an integer. 2nd arg not taken into account.")
        else:
            logger.info("Launch extraction with cycle time of 1200 seconds")
            cycle_time_sec = 1200
            operate_timer(cycle_time_sec=cycle_time_sec,
                          stop_time_sec=stop_time)
