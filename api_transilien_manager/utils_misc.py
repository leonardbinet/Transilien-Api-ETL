import os
from os import sys, path
import logging
from logging.handlers import RotatingFileHandler
from dateutil.tz import tzlocal
import pytz
from datetime import datetime
import numpy as np
import pandas as pd
from api_transilien_manager.settings import logs_path, data_path, responding_stations_path


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def compute_delay(scheduled_departure_time, real_departure_time):
    """
    Return in seconds the delay:
    - positive if real_time > schedule time (delayed)
    - negative if real_time < schedule time (advance)

    Return None if one value is not string

    Convert custom hours (after midnight) TODO
    """
    # real_departure_date = "01/02/2017 22:12" (api format)
    # scheduled_departure_time = '22:12:00' (schedules format)
    # real_departure_time = "22:12:00"
    # We don't need to take into account time zones

    # Accept only string
    if pd.isnull(scheduled_departure_time) or pd.isnull(real_departure_time):
        return None

    try:
        scheduled_departure_time = str(scheduled_departure_time)
        real_departure_time = str(real_departure_time)
    except Exception as e:
        print(e)
        return None

    # Take into account 24->29 => 0->5
    real_hour = int(real_departure_time[:2])
    if real_hour >= 24:
        real_hour -= 24
        real_departure_time = "%s%s" % (
            str(real_hour), real_departure_time[2:])

    sched_hour = int(scheduled_departure_time[:2])
    if sched_hour >= 24:
        sched_hour -= 24
        scheduled_departure_time = "%s%s" % (
            str(sched_hour), scheduled_departure_time[2:])

    # Convert into datime objects
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


def get_responding_stations_from_sample(sample_loc=None, write_loc=None):
    """
    This function's purpose is to write down responding stations from a given "real_departures" sample, and to write it down so it can be used to query only necessary stations (and avoid to spend API credits on unnecessary stations)
    """
    if not sample_loc:
        sample_loc = path.join(data_path, "20170131_real_departures.csv")
    if not write_loc:
        write_loc = responding_stations_path

    df = pd.read_csv(sample_loc)
    resp_stations = df["station"].unique()
    np.savetxt(write_loc, resp_stations, delimiter=",", fmt="%s")

    return list(resp_stations)
