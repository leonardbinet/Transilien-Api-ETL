import os
from os import sys, path
import pandas as pd
import zipfile
from urllib.request import urlretrieve
import logging
import json

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from api_transilien_manager.utils_misc import set_logging_conf
    set_logging_conf(log_name="mod_01_extract_schedule.log")

from api_transilien_manager.settings import BASE_DIR, data_path, gtfs_path, gtfs_csv_url
from api_transilien_manager.utils_mongo import mongo_async_upsert_items
from api_transilien_manager.utils_rdb import rdb_connection


logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None


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

    trips["train_num"] = trips["trip_id"].str.extract(
        "^.{5}(\d{6})", expand=False)

    df_merged = stop_times.merge(trips, on="trip_id", how="left")
    df_merged = df_merged.merge(calendar, on="service_id", how="left")
    df_merged = df_merged.merge(stops, on="stop_id", how="left")

    df_merged["station_id"] = df_merged.stop_id.str.extract(
        "DUA(\d{7})", expand=False)

    df_merged.rename(
        columns={'departure_time': 'scheduled_departure_time'}, inplace=True)

    useful = [
        "trip_id", "scheduled_departure_time", "station_id", "service_id",
        "monday", "tuesday", "wednesday",
        "thursday", "friday", "saturday", "sunday",
        "start_date", "end_date", "train_num"
    ]
    df_merged[useful].to_csv(os.path.join(gtfs_path, "flat.csv"))


def save_all_schedule_tables_rdb():
    save_trips_extended_rdb()
    save_stop_times_extended_rdb()


def save_trips_extended_rdb():
    """
    Save trips table, with some more columns:
    - train_num
    - calendar columns
    """
    trips = pd.read_csv(path.join(gtfs_path, "trips.txt"))
    logger.debug("Found %d trips." % len(trips.index))
    calendar = pd.read_csv(path.join(gtfs_path, "calendar.txt"))

    trips["train_num"] = trips["trip_id"].str.extract(
        "^.{5}(\d{6})", expand=False)
    extended = trips.merge(calendar, on="service_id", how="left")

    # Clean missing fields
    # Block id is always NaN, and drop where calendar is not present
    del extended['block_id']
    extended.dropna(axis=0, how='any', inplace=True)

    logger.debug("%d trips with calendar dates." % len(extended.index))

    connection = rdb_connection(db="postgres_alch")
    extended.to_sql("trips_ext", connection, if_exists='replace',
                    index=False, index_label="trip_id", chunksize=1000)
    return extended


def save_stop_times_extended_rdb():
    """
    Save stop times table, with some more columns:
    - train_num (out of trip_id)
    - calendar columns
    - station_id column (7 digits out of stop_id)
    - stops columns, (stop name for instance)
    """
    trips = pd.read_csv(path.join(gtfs_path, "trips.txt"))
    calendar = pd.read_csv(path.join(gtfs_path, "calendar.txt"))
    stop_times = pd.read_csv(path.join(gtfs_path, "stop_times.txt"))
    stops = pd.read_csv(path.join(gtfs_path, "stops.txt"))

    stop_times_ext = stop_times.merge(trips, on="trip_id", how="left")
    stop_times_ext = stop_times_ext.merge(
        calendar, on="service_id", how="left")
    stop_times_ext = stop_times_ext.merge(stops, on="stop_id", how="left")

    stop_times_ext["train_num"] = stop_times_ext[
        "trip_id"].str.extract("^.{5}(\d{6})", expand=False)
    stop_times_ext["station_id"] = stop_times_ext.stop_id.str.extract(
        "DUA(\d{7})", expand=False)

    # useful = [
    #    "trip_id", "departure_time", "station_id", "service_id",
    #    "monday", "tuesday", "wednesday",
    #    "thursday", "friday", "saturday", "sunday",
    #    "start_date", "end_date", "train_num"
    #]
    connection = rdb_connection(db="postgres_alch")
    stop_times_ext.to_sql("stop_times_ext", connection, if_exists='replace',
                          index=True, index_label=None, chunksize=1000)
