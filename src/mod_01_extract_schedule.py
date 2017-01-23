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
