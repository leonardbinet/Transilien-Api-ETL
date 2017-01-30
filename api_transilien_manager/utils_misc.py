import os
from os import sys, path
import logging
from logging.handlers import RotatingFileHandler
from dateutil.tz import tzlocal
import pytz
from datetime import datetime
import numpy as np
from api_transilien_manager.settings import BASE_DIR, logs_path


def compute_delay(scheduled_departure_time, real_departure_time):
    """
    Return in seconds the delay:
    - positive if real_time > schedule time (delayed)
    - negative if real_time < schedule time (advance)
    """
    # real_departure_date = "01/02/2017 22:12" (api format)
    # scheduled_departure_time = '22:12:00' (schedules format)
    # real_departure_time = "22:12:00"
    # We don't need to take into account time zones

    real_departure_time = datetime.strptime(
        real_departure_time, "%H:%M:%S")

    scheduled_departure_time = datetime.strptime(
        scheduled_departure_time, "%H:%M:%S")

    # If late: delay is positive, if in advance, it is negative
    delay = real_departure_time - scheduled_departure_time
    # If schedule is 23:59:59 and the real is 00:00:01, bring back to 2 secs
    # If real is 23:59:59 and the schedule is 00:00:01, bring back to -2 secs
    secs_in_day = 60 * 60 * 24
    if abs(delay.seconds) > secs_in_day / 2:
        real_delay = np.sign(delay.seconds) * (-1) * \
            abs(secs_in_day - delay.seconds)
    else:
        real_delay = delay.seconds
    return real_delay


def get_paris_local_datetime_now():
    paris_tz = pytz.timezone('Europe/Paris')
    datetime_paris = datetime.now(tzlocal()).astimezone(paris_tz)
    return datetime_paris


def set_logging_conf(log_name, level="INFO"):
    """
    This must be imported by all scripts running as "main"
    """
    if level == "INFO":
        level = logging.INFO
    elif level == "DEBUG":
        level = logging.DEBUG
    else:
        level = logging.INFO
    # Delete all previous potential handlers
    logger = logging.getLogger()
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Set config
    # logging_file_path = os.path.join(logs_path, log_name)
    logging_file_path = path.join(logs_path, log_name)

    # création d'un handler qui va rediriger une écriture du log vers
    # un fichier en mode 'append', avec 1 backup et une taille max de 1Mo
    file_handler = RotatingFileHandler(logging_file_path, 'a', 1000000, 1)

    # création d'un second handler qui va rediriger chaque écriture de log
    # sur la console
    stream_handler = logging.StreamHandler()

    handlers = [file_handler, stream_handler]

    logging.basicConfig(
        format='%(asctime)s-- %(name)s -- %(levelname)s -- %(message)s', level=level, handlers=handlers)

    # Python crashes or captured as well (beware of ipdb imports)
    def handle_exception(exc_type, exc_value, exc_traceback):
        # if issubclass(exc_type, KeyboardInterrupt):
        #    sys.__excepthook__(exc_type, exc_value, exc_traceback)
        logging.error("Uncaught exception : ", exc_info=(
            exc_type, exc_value, exc_traceback))
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    sys.excepthook = handle_exception
