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
from src.settings import BASE_DIR
from src.utils_mongo import mongo_async_upsert_chunks


logger = logging.getLogger(__name__)

# CONFIG
data_path = os.path.join(BASE_DIR, "data")
gtfs_path = os.path.join(data_path, "gtfs-lines-last")
gtfs_csv_url = 'https://ressources.data.sncf.com/explore/dataset/sncf-transilien-gtfs/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true'


def download_gtfs_files():
    logger.info(
        "Download of csv containing links of zip files, at url %s" % gtfs_csv_url)
    df_links_gtfs = pd.read_csv(gtfs_csv_url)

    for link in df_links_gtfs["file"].values:
        logger.info("Download of %s" % link)
        local_filename, headers = urlretrieve(link)

        logger.info("File name is %s" % headers.get_filename())
        # Get name in header and remove the ".zip"
        extracted_data_folder_name = headers.get_filename().split(".")[0]

        with zipfile.ZipFile(local_filename, "r") as zip_ref:
            full_path = os.path.join(data_path, extracted_data_folder_name)
            zip_ref.extractall(path=full_path)


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
        "trip_num", "scheduled_departure_time", "station_id",
        "monday", "tuesday", "wednesday",
        "thursday", "friday", "saturday", "sunday",
        "start_date", "end_date", "train_id"
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


def get_departure_times_df_of_day(yyyymmdd_format, stop_filter=None, station_filter=None):
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

    if stop_filter:
        cond2 = matching_stop_times["stop_id"].isin(stop_filter)
        matching_stop_times = matching_stop_times[cond2]

    if station_filter:
        cond3 = matching_stop_times["station_id"].isin(station_filter)
        matching_stop_times = matching_stop_times[cond3]

    return matching_stop_times


def save_scheduled_departures_of_day_mongo(yyyymmdd_format):
    df_departure_times = get_departure_times_df_of_day(yyyymmdd_format)
    data_json = json.loads(df_departure_times.to_json(orient='records'))

    index_fields = ["scheduled_departure_day", "station_id", "train_num"]

    logger.info("Upsert of %d items of json data in Mongo scheduled_departures collection" %
                len(data_json))

    mongo_async_upsert_chunks(
        "scheduled_departures", data_json, index_fields)
