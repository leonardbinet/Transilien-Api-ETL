import os
from os import sys, path
import pandas as pd
import datetime
import calendar
import zipfile
from urllib.request import urlretrieve
import logging
import json
import pytz
from src.settings import BASE_DIR, data_path, gtfs_path, gtfs_csv_url
from src.utils_mongo import mongo_async_upsert_items
from src.utils_rdb import sqlite_get_connection

logger = logging.getLogger(__name__)


def trip_scheduled_departure_time(trip_id, station):
    # Check parameters
    if len(str(station)) == 8:
        station = str(station)[:-1]
    elif len(str(station)) == 7:
        station = str(station)
    else:
        logger.warn("Station must be 7 digits (8 accepted)")
        return False

    # Make query
    connection = sqlite_get_connection()
    cursor = connection.cursor()
    query = "SELECT departure_time FROM stop_times_ext WHERE trip_id=? AND station_id=?;"
    cursor.execute(query, (trip_id, station))
    departure_time = cursor.fetchone()
    connection.close()

    # Check number of results
    if not departure_time:
        logger.warning("No matching scheduled_departure_time")
        return False
    elif len(departure_time) == 0:
        logger.warning("No matching scheduled_departure_time")
        return False
    elif len(departure_time) == 1:
        departure_time = departure_time[0]
        logger.debug("Found departure time: %s" % departure_time)
        return departure_time
    else:
        logger.warning("Multiple scheduled time found: %d matches" %
                       len(departure_time))
        return False
    return departure_time


def sqlite_get_departure_times_of_day_json_list(yyyymmdd_format):
    # Check passed parameters
    try:
        int(yyyymmdd_format)
    except Exception as e:
        logger.error("Date provided is not valid %s, %s" %
                     (yyyymmdd_format, e))
        return False
    if len(yyyymmdd_format) != 8:
        logger.error("Date provided is not valid %s" %
                     (yyyymmdd_format))
        return False

    # Find weekday for day condition
    datetime_format = datetime.datetime.strptime(yyyymmdd_format, "%Y%m%d")
    weekday = calendar.day_name[datetime_format.weekday()].lower()

    # Make query
    connection = sqlite_get_connection()
    query = "SELECT * FROM stop_times_ext WHERE start_date<=%s AND end_date>=%s;" % (
        yyyymmdd_format, yyyymmdd_format)
    matching_stop_times = pd.read_sql(query, connection)

    # Rename departure_time column, and add requested date
    matching_stop_times["scheduled_departure_day"] = yyyymmdd_format
    matching_stop_times.rename(
        columns={'departure_time': 'scheduled_departure_time'}, inplace=True)

    json_list = json.loads(matching_stop_times.to_json(orient='records'))
    logger.info("There are %d scheduled departures on %s" %
                (len(json_list), yyyymmdd_format))
    return json_list


def get_services_of_day(yyyymmdd_format):
    all_services = pd.read_csv(os.path.join(gtfs_path, "calendar.txt"))
    datetime_format = datetime.datetime.strptime(yyyymmdd_format, "%Y%m%d")
    weekday = calendar.day_name[datetime_format.weekday()].lower()

    cond1 = all_services[weekday] == 1
    cond2 = all_services["start_date"] <= int(yyyymmdd_format)
    cond3 = all_services["end_date"] >= int(yyyymmdd_format)

    matching_services = all_services[cond1][cond2][cond3]

    return list(matching_services["service_id"].values)


def get_trips_of_day(yyyymmdd_format):
    all_trips = pd.read_csv(os.path.join(gtfs_path, "trips.txt"))
    services_on_day = get_services_of_day(
        yyyymmdd_format)
    trips_condition = all_trips["service_id"].isin(services_on_day)
    trips_on_day = list(all_trips[trips_condition]["trip_id"].unique())
    return trips_on_day


def get_departure_times_of_day_json_list(yyyymmdd_format, stop_filter=None, station_filter=None):
    """
    stop_filter is a list of stops you want, it must be in GTFS format:
    station_filter is a list of stations you want, it must be api format
    """

    all_stop_times = pd.read_csv(os.path.join(gtfs_path, "stop_times.txt"))
    trips_on_day = get_trips_of_day(yyyymmdd_format)

    cond1 = all_stop_times["trip_id"].isin(trips_on_day)
    matching_stop_times = all_stop_times[cond1]

    matching_stop_times["scheduled_departure_day"] = yyyymmdd_format
    matching_stop_times.rename(
        columns={'departure_time': 'scheduled_departure_time'}, inplace=True)
    matching_stop_times["station_id"] = matching_stop_times[
        "stop_id"].str.extract("DUA(\d{7})")
    matching_stop_times["train_num"] = matching_stop_times[
        "trip_id"].str.extract("^.{5}(\d{6})")

    if stop_filter:
        cond2 = matching_stop_times["stop_id"].isin(stop_filter)
        matching_stop_times = matching_stop_times[cond2]

    if station_filter:
        cond3 = matching_stop_times["station_id"].isin(station_filter)
        matching_stop_times = matching_stop_times[cond3]

    json_list = json.loads(matching_stop_times.to_json(orient='records'))
    logger.info("There are %d scheduled departures on %s" %
                (len(json_list), yyyymmdd_format))
    return json_list


def save_scheduled_departures_of_day_mongo(yyyymmdd_format):
    json_list = get_departure_times_of_day_json_list(yyyymmdd_format)

    index_fields = ["scheduled_departure_day", "station_id", "train_num"]

    logger.info(
        "Upsert of %d items of json data in Mongo scheduled_departures collection" % len(json_list))

    mongo_async_upsert_items("scheduled_departures", json_list, index_fields)
