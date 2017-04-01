"""
Module containing some useful functions that might be used by all other
modules.
"""

from os import sys, path
import logging
from logging.handlers import RotatingFileHandler
from dateutil.tz import tzlocal
import pytz
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import numpy as np
import pandas as pd

from api_etl.settings import (
    data_path, responding_stations_path,
    all_stations_path, top_stations_path,
    scheduled_stations_path, logs_path
)

logger = logging.getLogger(__name__)


def build_uri(
    db_type, host, user=None, password=None,
    port=None, database=None
):
    uri = "%s://" % db_type
    if user and password:
        uri += "%s:%s@" % (quote_plus(user), quote_plus(password))
    uri += host
    if port:
        uri += ":" + str(port)
    if database:
        uri += "/%s" % quote_plus(database)
    return uri


def chunks(l, n):
    """
    Yield a list in 'n' lists of nearly same size (some can be one more than
    others).

    :param l: list you want to divide in chunks
    :type l: list

    :param n: number of chunks you want to get
    :type n: int
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


class StationProvider():
    """ Class to easily get lists of stations in gtfs format (7 digits) or
    transilien's format (8 digits).

    Warning: data sources have to be checked ("all" is ok, "top" is wrong).
    """

    def __init__(self):
        self._all_stations_path = all_stations_path
        self._responding_stations_path = responding_stations_path
        self._top_stations_path = top_stations_path
        self._scheduled_stations_path = scheduled_stations_path

    def get_station_ids(self, stations="all", gtfs_format=False):
        """
        Get stations ids either in API format (8 digits), or in GTFS format
        (7 digits).

        Beware, this function has to be more tested.
        Beware: two formats:
        - 8 digits format to query api
        - 7 digits format to query gtfs files
        """
        if stations == "all":
            station_path = self._all_stations_path

        elif stations == "responding":
            station_path = self._responding_stations_path

        elif stations == "top":
            station_path = self._top_stations_path

        elif stations == "scheduled":
            station_path = self._scheduled_stations_path

        else:
            raise ValueError(
                "stations parameter should be either 'all', 'top',"
                + " 'scheduled' or 'responding'"
            )

        station_ids = np.genfromtxt(station_path, delimiter=",", dtype=str)

        if gtfs_format:
            # Remove last character
            station_ids = map(lambda x: x[:-1], station_ids)

        return list(station_ids)


class DateConverter():
    """Class to convert dates from and to our special format, from and to api
    date format, and to and from our regular format:
    \n- api_format: "16/02/2017 01:26"
    \n- normal date: "20170216"
    \n- normal time: "01:26:00"
    \n- special date: "20170215"
    \n- special time: "25:26:00"

    \nThis class has also methods to compute delays
    """

    def __init__(
        self, dt=None, api_date=None, normal_date=None, normal_time=None,
        special_date=None, special_time=None
    ):
        """Works in two steps, first try to find real datetime from arguments
        passed, then computes string representations.
        """
        self.dt = dt
        self.api_date = api_date
        self.normal_date = normal_date
        self.normal_time = normal_time
        self.special_date = special_date
        self.special_time = special_time

        if self.api_date:
            self._api_date_to_dt()

        elif (self.normal_date and self.normal_time):
            self._normal_datetime_to_dt()

        elif (self.special_time and self.special_date):
            self._special_datetime_to_dt()

        else:
            assert self.dt

        self.api_date = self.dt.strftime("%d/%m/%Y %H:%M")
        self.normal_date = self.dt.strftime("%Y%m%d")
        self.normal_time = self.dt.strftime("%H:%M:%S")
        # Compute special datetime self.special_date and self.special_time
        self._dt_to_special_datetime()

    def _api_date_to_dt(self):
        assert self.api_date
        self.dt = datetime.strptime(self.api_date, "%d/%m/%Y %H:%M")

    def _normal_datetime_to_dt(self):
        assert (self.normal_date and self.normal_time)
        # "2017021601:26:00"
        full_str_dt = "%s%s" % (self.normal_date, self.normal_time)
        self.dt = datetime.strptime(full_str_dt, "%Y%m%d%H:%M:%S")

    def _special_datetime_to_dt(self):
        assert(self.special_date and self.special_time)
        hour = self.special_time[:2]
        assert (int(hour) >= 0 and int(hour) < 29)
        add_day = False
        if int(hour) in (24, 25, 26, 27):
            hour = str(int(hour) - 24)
            add_day = True
        corr_sp_t = hour + self.special_time[2:]
        full_str_dt = "%s%s" % (self.special_date, corr_sp_t)
        dt = datetime.strptime(full_str_dt, "%Y%m%d%H:%M:%S")
        if add_day:
            dt = dt + timedelta(days=1)
        self.dt = dt

    def _dt_to_special_datetime(self):
        """
        Dates between 0 and 3 AM are transformed in +24h time format with
        day as previous day.
        """
        assert self.dt
        # For hours between 00:00:00 and 02:59:59: we add 24h and say it
        # is from the day before
        if self.dt.hour in (0, 1, 2):
            # say this train is departed the day before
            special_dt = self.dt - timedelta(days=1)
            self.special_date = special_dt.strftime("%Y%m%d")
            # +24: 01:44:00 -> 25:44:00
            self.special_time = "%s:%s" % (
                self.dt.hour + 24, self.dt.strftime("%M:%S"))
        else:
            self.special_date = self.dt.strftime("%Y%m%d")
            self.special_time = self.dt.strftime("%H:%M:%S")

    def compute_delay_from(
        self, dc=None, dt=None, api_date=None, normal_date=None,
        normal_time=None, special_date=None, special_time=None
    ):
        """
        Create another DateConverter and compares datetimes
        Return in seconds the delay:
        - positive if this one > 'from' (delayed)
        - negative if this one < 'from' (advance)
        """
        if dc:
            assert isinstance(dc, DateConverter)
            other_dt = dc.dt
        else:
            other_dt = DateConverter(
                api_date=api_date, normal_date=normal_date,
                normal_time=normal_time, dt=dt,
                special_date=special_date, special_time=special_time
            ).dt
        time_delta = self.dt - other_dt

        return time_delta.total_seconds()


def get_paris_local_datetime_now():
    """
    Return paris local time (necessary for operations operated on other time
    zones)
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
        format='%(asctime)s-- %(name)s -- %(levelname)s -- %(message)s',
        level=level, handlers=handlers
    )

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
    This function's purpose is to write down responding stations from a given
    "real_departures" sample, and to write it down so it can be used to query
    only necessary stations (and avoid to spend API credits on unnecessary
    stations)
    """
    if not sample_loc:
        sample_loc = path.join(data_path, "20170131_real_departures.csv")
    if not write_loc:
        write_loc = responding_stations_path

    df = pd.read_csv(sample_loc)
    resp_stations = df["station"].unique()
    np.savetxt(write_loc, resp_stations, delimiter=",", fmt="%s")

    return list(resp_stations)
