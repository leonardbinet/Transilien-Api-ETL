import os
import logging
from os import sys, path

from settings import BASE_DIR

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    from src.utils_misc import set_logging_conf
    set_logging_conf(log_name="task_01_long_cycle.log")


from src.mod_01_extract import operate_multiple_cycles

logger = logging.getLogger(__name__)

# By default, run for one hour (minus 100 sec), every 2 minutes
# max 300 queries per sec
operate_multiple_cycles(station_filter=False, cycle_time_sec=120,
                        stop_time_sec=3500, max_per_minute=300)
