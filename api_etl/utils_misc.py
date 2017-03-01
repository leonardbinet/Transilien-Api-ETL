"""
Module containing some useful functions that might be used by all other modules.
"""

from os import sys, path
import logging
from logging.handlers import RotatingFileHandler
from dateutil.tz import tzlocal
import pytz
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from api_etl.settings import logs_path, data_path, responding_stations_path

from api_etl.settings import data_path, col_real_dep_unique, responding_stations_path, all_stations_path, dynamo_real_dep, top_stations_path, scheduled_stations_path


def chunks(l, n):
    """
    Yield a list in 'n' lists of nearly same size (some can be one more than others).

    :param l: list you want to divide in chunks
    :type l: list

    :param n: number of chunks you want to get
    :type n: int
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_station_ids(stations="all"):
    """
    Get stations ids either in API format (8 digits), or in GTFS format (7 digits).

    Beware, this function has to be more tested.
    Beware: two formats:
    - 8 digits format to query api
    - 7 digits format to query gtfs files
    """
    if stations == "all":
        station_ids = np.genfromtxt(
            all_stations_path, delimiter=',', dtype=str)

    elif stations == "responding":
        station_ids = np.genfromtxt(
            responding_stations_path, delimiter=',', dtype=str)

    elif stations == "top":
        station_ids = np.genfromtxt(
            top_stations_path, delimiter=',', dtype=str)

    elif stations == "scheduled":
        station_ids = np.genfromtxt(
            scheduled_stations_path, delimiter=",", dtype=str)

    else:
        raise ValueError(
            "stations parameter should be either 'all', 'top', 'scheduled' or 'responding'")

    return list(station_ids)


def api_date_to_day_time_corrected(api_date, time_or_day):
    """
    Function that transform dates given by api in usable fields:
    - expected_passage_day : "20120523" format
    - expected_passage_time: "12:55:00" format

    Important: dates between 0 and 3 AM are transformed in +24h time format with day as previous day.

    :param api_date: api date you want to transform
    :type api_date: str

    :param time_or_day: choose either "time" or "day", do you want to get corrected day or time?
    :type time_or_day: str ("time", or "day")
    """
    expected_passage_date = datetime.strptime(api_date, "%d/%m/%Y %H:%M")

    day_string = expected_passage_date.strftime("%Y%m%d")
    time_string = expected_passage_date.strftime("%H:%M:00")

    # For hours between 00:00:00 and 02:59:59: we add 24h and say it
    # is from the day before
    if expected_passage_date.hour in (0, 1, 2):
        # say this train is departed the time before
        expected_passage_date = expected_passage_date - timedelta(days=1)
        # overwrite day_string
        day_string = expected_passage_date.strftime("%Y%m%d")
        # overwrite time_string with +24: 01:44:00 -> 25:44:00
        time_string = "%d:%d:00" % (
            expected_passage_date.hour + 24, expected_passage_date.minute)

    if time_or_day == "day":
        return day_string
    elif time_or_day == "time":
        return time_string
    else:
        raise ValueError("time or day should be 'time' or 'day'")


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
    """
    Return paris local time (necessary for operations operated on other time zones)
    """
    paris_tz = pytz.timezone('Europe/Paris')
    datetime_paris = datetime.now(tzlocal()).astimezone(paris_tz)
    return datetime_paris


def set_logging_conf(log_name, level="INFO"):
    """
    This function sets the logging configuration.
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
