import os
from os import path, sys
import sys
import logging


if __name__ == "__main__":
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from src.utils_misc import set_logging_conf
import src.mod_02_find_schedule

set_logging_conf("log_test.txt")
logger = logging.getLogger(__name__)

logger.info("Seems to work")

raise RuntimeError("Test unhandled")
