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


logger = logging.getLogger(__name__)


def download_gtfs_files():
    logger.info(
        "Download of csv containing links of zip files, at url %s" % gtfs_csv_url)
    df_links_gtfs = pd.read_csv(gtfs_csv_url)

    # Download and unzip all files
    # Check if one is "gtfs-lines-last" (necessary)
    gtfs_lines_last_present = False
    for link in df_links_gtfs["file"].values:
        logger.info("Download of %s" % link)
        local_filename, headers = urlretrieve(link)

        logger.info("File name is %s" % headers.get_filename())
        # Get name in header and remove the ".zip"
        extracted_data_folder_name = headers.get_filename().split(".")[0]
        if extracted_data_folder_name == "gtfs-lines-last":
            gtfs_lines_last_present = True
            logger.info("The 'gtfs-lines-last' folder has been found.")

        with zipfile.ZipFile(local_filename, "r") as zip_ref:
            full_path = os.path.join(data_path, extracted_data_folder_name)
            zip_ref.extractall(path=full_path)

        if not gtfs_lines_last_present:
            logger.error(
                "The 'gtfs-lines-last' folder has been found! Schedules will not be updated.")


def write_flat_departures_times_df():
    try:
        trips = pd.read_csv(path.join(gtfs_path, "trips.txt"))
        calendar = pd.read_csv(path.join(gtfs_path, "calendar.txt"))
        stop_times = pd.read_csv(path.join(gtfs_path, "stop_times.txt"))
        stops = pd.read_csv(path.join(gtfs_path, "stops.txt"))

    except OSError:
        logger.info("Could not load files: download files from the internet.")
        download_gtfs_files()

        trips = pd.read_csv(path.join(gtfs_path, "trips.txt"))
        calendar = pd.read_csv(path.join(gtfs_path, "calendar.txt"))
        stop_times = pd.read_csv(path.join(gtfs_path, "stop_times.txt"))
        stops = pd.read_csv(path.join(gtfs_path, "stops.txt"))

    trips["train_num"] = trips["trip_id"].str.extract("^.{5}(\d{6})")

    df_merged = stop_times.merge(trips, on="trip_id", how="left")
    df_merged = df_merged.merge(calendar, on="service_id", how="left")
    df_merged = df_merged.merge(stops, on="stop_id", how="left")

    df_merged["station_id"] = df_merged.stop_id.str.extract("DUA(\d{7})")

    df_merged.rename(
        columns={'departure_time': 'scheduled_departure_time'}, inplace=True)

    useful = [
        "trip_id", "scheduled_departure_time", "station_id", "service_id",
        "monday", "tuesday", "wednesday",
        "thursday", "friday", "saturday", "sunday",
        "start_date", "end_date", "train_num"
    ]
    df_merged[useful].to_csv(os.path.join(gtfs_path, "flat.csv"))


def get_flat_departures_times_df():
    try:
        df_merged = pd.read_csv(path.join(gtfs_path, "flat.csv"))
    except:
        logger.info("Flat csv not found, let's create it")
        write_flat_departures_times_df()
        df_merged = pd.read_csv(path.join(gtfs_path, "flat.csv"))
    return df_merged


def trip_scheduled_departure_time(trip_id, station):
    """
    Get trip scheduled_departure_time from trip_id and station.
    Station provided must be 7 digits format (8 accepted).
    Trip scheduled departures times are day-agnostic.
    """
    if len(str(station)) == 8:
        station = str(station)[:-1]
    elif len(str(station)) == 7:
        station = str(station)
    else:
        logger.warn("Station must be 7 digits (8 accepted)")
        return False

    logger.debug("Trying to find departure time for trip_id %s for station_id %s" % (
        trip_id, station))

    dep_times = pd.read_csv(path.join(gtfs_path, "stop_times.txt"))
    cond_trip = dep_times["trip_id"] == str(trip_id)
    dep_times = dep_times[cond_trip]
    logger.debug("%d row(s) after trip_id filtering." % len(dep_times.index))

    # Find station_id from stop_id
    dep_times["station_id"] = dep_times["stop_id"].str.extract("DUA(\d{7})")
    cond_station = dep_times["station_id"] == station
    dep_times = dep_times[cond_station]
    logger.debug("%d row(s) after station_id filtering." %
                 len(dep_times.index))

    dep_times = list(dep_times["departure_time"].unique())

    n = len(dep_times)
    if n == 0:
        logger.warning("No matching scheduled_departure_time")
        return False
    elif n == 1:
        dep_times = dep_times[0]
        logger.debug("Found departure time: %s" % dep_times)
        return dep_times
    else:
        logger.warning("Multiple scheduled time found: %d matches" % n)
        return False


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
    return json_list


def save_scheduled_departures_of_day_mongo(yyyymmdd_format):
    json_list = get_departure_times_of_day_json_list(yyyymmdd_format)

    index_fields = ["scheduled_departure_day", "station_id", "train_num"]

    logger.info(
        "Upsert of %d items of json data in Mongo scheduled_departures collection" % len(json_list))

    mongo_async_upsert_items("scheduled_departures", json_list, index_fields)
