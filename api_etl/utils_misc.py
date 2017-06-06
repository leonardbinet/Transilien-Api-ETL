"""
Module containing some useful functions that might be used by all other
modules.
"""

import os
from os import sys, path, listdir
from os.path import isfile, join
import logging
from logging.handlers import RotatingFileHandler
from dateutil.tz import tzlocal
import pytz
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import boto3
import botocore

if __name__ == '__main__':
    import logging.config
    logging.config.fileConfig('logging.conf')

from api_etl.settings import (
    data_path, responding_stations_path,
    all_stations_path, top_stations_path,
    scheduled_stations_path, logs_path,
    stations_per_line_path
)

from api_etl.utils_secrets import get_secret


AWS_DEFAULT_REGION = get_secret("AWS_DEFAULT_REGION", env=True)
AWS_ACCESS_KEY_ID = get_secret("AWS_ACCESS_KEY_ID", env=True)
AWS_SECRET_ACCESS_KEY = get_secret("AWS_SECRET_ACCESS_KEY", env=True)


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
        self._stations_per_line_path = stations_per_line_path

    def get_stations_per_line(self, lines=None, UIC7=False, full_df=False):
        """
        Get stations of given line (multiple lines possible)
        """
        if lines:
            assert isinstance(lines, list)

        lines = lines or ['C', 'D', 'E', 'H', 'J', 'K', 'N', 'P', 'U']
        # all but 'A', 'Aéroport C', 'B', 'T4', 'L', 'R'
        station_path = self._stations_per_line_path
        df = pd.read_csv(station_path, sep=";")
        matching_stop_times = df.dropna(axis=0, how="all", subset=lines)

        if full_df:
            return matching_stop_times

        stations = matching_stop_times.Code_UIC.apply(str).tolist()
        if not UIC7:
            return stations

        return list(map(lambda x: x[0: -1], stations))

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
        special_date=None, special_time=None, force_regular_date=False
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
            self._special_datetime_to_dt(force_regular_date)

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

    def _special_datetime_to_dt(self, force_regular_date):
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
        if add_day and not force_regular_date:
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
        normal_time=None, special_date=None, special_time=None,
        force_regular_date=False
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
                special_date=special_date, special_time=special_time, force_regular_date=force_regular_date
            ).dt
        time_delta = self.dt - other_dt

        return time_delta.total_seconds()


def get_paris_local_datetime_now(tz_naive=True):
    """
    Return paris local time (necessary for operations operated on other time
    zones)
    """
    paris_tz = pytz.timezone('Europe/Paris')
    datetime_paris = datetime.now(tzlocal()).astimezone(paris_tz)
    if tz_naive:
        return datetime_paris.replace(tzinfo=None)
    else:
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

# S3 functions


def s3_ressource():
    # Credentials are accessed via environment variables
    s3 = boto3.resource('s3', region_name=AWS_DEFAULT_REGION)
    return s3


class S3Bucket():

    def __init__(self, name, create_if_absent=False):
        self._s3 = s3_ressource()
        self.bucket_name = name
        self._check_if_accessible()
        if create_if_absent and not self._accessible:
            self._create_bucket()

    def _check_if_accessible(self):
        try:
            self._s3.meta.client.head_bucket(Bucket=self.bucket_name)
            self._accessible = True
            self.bucket = self._s3.Bucket(self.bucket_name)
            logging.info("Bucket %s is accessible." % self.bucket_name)
            return True

        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            self._accessible = False
            logging.info("Bucket %s does not exist." % self.bucket_name)
            error_code = int(e.response['Error']['Code'])
            logging.debug("Could not access bucket %s: %s" %
                          (self.bucket_name, e.response))
            return False

    def _create_bucket(self):
        assert not self._accessible
        logging.info("Creating bucket %s" % self.bucket_name)
        self._s3.create_bucket(
            Bucket=self.bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': AWS_DEFAULT_REGION}
        )
        self._check_if_accessible()

    def send_file(self, file_path, file_name=None, delete=False, ignore_hidden=False):
        if not file_name:
            file_name = file_path

        if ignore_hidden:
            n = os.path.basename(os.path.normpath(file_path))
            if n.startswith("."):
                return None

        logging.info("Saving file '%s', as '%s' in bucket '%s'." %
                     (file_path, file_name, self.bucket_name))
        self._s3.Object(self.bucket_name, file_name)\
            .put(Body=open(file_path, 'rb'))

        if delete:
            os.remove(file_path)

    def send_folder(self, folder_path, folder_name=None, delete=False, ignore_hidden=True):
        """Will keep same names for files inside folder.

        Note: in S3, there is no folder, just files with names as path.
        """
        # if no new name specified, use existing name
        if not folder_name:
            n = path.relpath(folder_path)
            folder_name = n

        if ignore_hidden:
            n = os.path.basename(os.path.normpath(folder_path))
            if n.startswith("."):
                return None

        logging.info("Saving folder '%s', as '%s' in bucket '%s'." %
                     (folder_path, folder_name, self.bucket_name))

        files = [f for f in listdir(
            folder_path) if isfile(join(folder_path, f))]
        subfolders = [f for f in listdir(
            folder_path) if not isfile(join(folder_path, f))]

        # new file names:

        for f in files:
            self.send_file(
                file_path=join(folder_path, f),
                file_name=join(folder_name, f),
                delete=delete
            )

        for subf in subfolders:
            self.send_folder(
                folder_path=join(folder_path, subf),
                folder_name=join(folder_name, subf),
                delete=delete
            )

        if delete:
            os.rmdir(folder_path)

    def list_bucket_objects(self):
        self.bucket_objects = []
        for obj in self.bucket.objects.all():
            self.bucket_objects.append(obj.key)
            print(obj.key)
