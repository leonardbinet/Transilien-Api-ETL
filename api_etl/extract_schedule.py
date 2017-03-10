"""
Module used to download from SNCF website trains schedules and save it in the right format in different databases (Dynamo or relational database)
"""

import os
from os import sys, path
import pandas as pd
import zipfile
import time
from urllib.request import urlretrieve
import logging
import json
import datetime
import calendar


from api_etl.settings import BASE_DIR, data_path, gtfs_path, gtfs_csv_url, dynamo_sched_dep, shed_read, shed_write_on, shed_write_off
from api_etl.utils_mongo import mongo_async_upsert_items
from api_etl.utils_rdb import rdb_connection
from api_etl.utils_dynamo import dynamo_insert_batches, dynamo_update_provisionned_capacity

logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None


def download_gtfs_files():
    """
    Download gtfs files from SNCF website (based on URL defined in settings module) and saves it in data folder (defined as well in settings module). There is no paramater to pass.

    Process is in two steps:
    - first: download csv file containing links to zip files
    - second: download files based on urls found in csv from first step

    Folder names in which files are unzip are based on the headers of the zip files.

    Function returns True if 'gtfs-lines-last' folder has been found (this is the usual folder we use then to find schedules). Return False otherwise.

    :rtype: boolean
    """
    logger.info(
        "Download of csv containing links of zip files, at url %s", gtfs_csv_url)
    df_links_gtfs = pd.read_csv(gtfs_csv_url)

    # Download and unzip all files
    # Check if one is "gtfs-lines-last" (necessary)
    gtfs_lines_last_present = False
    for link in df_links_gtfs["file"].values:
        logger.info("Download of %s", link)
        local_filename, headers = urlretrieve(link)

        logger.info("File name is %s", headers.get_filename())
        # Get name in header and remove the ".zip"
        extracted_data_folder_name = headers.get_filename().split(".")[0]
        if extracted_data_folder_name == "gtfs-lines-last":
            gtfs_lines_last_present = True

        with zipfile.ZipFile(local_filename, "r") as zip_ref:
            full_path = os.path.join(data_path, extracted_data_folder_name)
            zip_ref.extractall(path=full_path)

        if gtfs_lines_last_present:
            logger.info("The 'gtfs-lines-last' folder has been found.")
            return True
        else:
            logger.error(
                "The 'gtfs-lines-last' folder has not been found! Schedules will not be updated.")
            return False


def get_services_of_day(yyyymmdd_format):
    """
    Given a date, this function will return all service-ids scheduled on transilien's network on this day.

    This function requires that gtfs files are present in data folder specified in settings module.

    :param yyyymmdd_format: date on yyyymmdd format
    :type yyyymmdd_format: string or int

    :rtype: list
    """
    # Get weekday: for double check
    datetime_format = datetime.datetime.strptime(yyyymmdd_format, "%Y%m%d")
    weekday = calendar.day_name[datetime_format.weekday()].lower()

    all_services = pd.read_csv(os.path.join(gtfs_path, "calendar.txt"))

    cond1 = all_services[weekday] == 1
    cond2 = all_services["start_date"] <= int(yyyymmdd_format)
    cond3 = all_services["end_date"] >= int(yyyymmdd_format)
    all_services = all_services[cond1 & cond2 & cond3]["service_id"].values

    # Get service exceptions
    # 1 = service (alors que normalement non)
    # 2 = pas serviec (alors que normalement oui)
    serv_exc = pd.read_csv(os.path.join(gtfs_path, "calendar_dates.txt"))
    serv_exc = serv_exc[serv_exc["date"] == int(yyyymmdd_format)]

    serv_add = serv_exc[serv_exc["exception_type"] == 1]["service_id"].values
    serv_rem = serv_exc[serv_exc["exception_type"] == 2]["service_id"].values

    serv_on_day = set(all_services)
    serv_on_day.update(serv_add)
    serv_on_day = serv_on_day - set(serv_rem)
    serv_on_day = list(serv_on_day)
    return serv_on_day


def get_trips_of_day(yyyymmdd_format):
    """
    Given a date, this function will return all trip-ids scheduled on transilien's network on this day.

    This function requires that gtfs files are present in data folder specified in settings module.

    :param yyyymmdd_format: date on yyyymmdd format
    :type yyyymmdd_format: string or int

    :rtype: list
    """
    all_trips = pd.read_csv(os.path.join(gtfs_path, "trips.txt"))
    services_on_day = get_services_of_day(
        yyyymmdd_format)
    trips_condition = all_trips["service_id"].isin(services_on_day)
    trips_on_day = list(all_trips[trips_condition]["trip_id"].unique())
    return trips_on_day


def get_departure_times_of_day_json_list(yyyymmdd_format, stop_filter=None, station_filter=None, df_format=False, dropna_index=["station_id", "day_train_num"]):
    """
    Given a date, this function will return all trip-ids scheduled on transilien's network on this day.

    This function requires that gtfs files are present in data folder specified in settings module.

    :param yyyymmdd_format: date on yyyymmdd format
    :type yyyymmdd_format: string or int

    :param stop_filter: default None. If set, should be a list of stops for which you want to obtain stop times scheduled on this day. Otherwise, if set to False or None, it will get all stops without restrictions on stations.
    :type stop_filter: None/False or list of valid stops_ids

    :param station_filter: default None. If set, should be a list of station ids for which you want to obtain stop times scheduled on this day. Otherwise, if set to False or None, it will get all stops without restrictions on stations.
    :type station_filter: None/False or list of valid station_filter

    :param df_format: default False. If set to True, will return a pandas dataframe
    :type df_format: boolean

    :param dropna_index: default ["station_id", "day_train_num"]. If set, it will drop all rows where the index fields might have NaN values or are duplicates of others rows.
    :type dropna_index: list or None

    :rtype: list of json serializable objects, or pandas dataframe if df_format is set to True
    """

    all_stop_times = pd.read_csv(os.path.join(gtfs_path, "stop_times.txt"))

    # Take either all lines, either only those for given day
    if yyyymmdd_format == "all":
        matching_stop_times = all_stop_times
    else:
        trips_on_day = get_trips_of_day(yyyymmdd_format)
        cond1 = all_stop_times["trip_id"].isin(trips_on_day)
        matching_stop_times = all_stop_times[cond1]
    # Add trips routes and agency fields
    # agency = pd.read_csv(os.path.join(gtfs_path, "agency.txt"))
    # agency = agency[["agency_id", "agency_name"]]
    routes = pd.read_csv(os.path.join(gtfs_path, "routes.txt"))
    routes = routes[["route_id", "route_short_name"]]
    # routes = routes.merge(agency, on="agency_id", how="inner")
    trips = pd.read_csv(os.path.join(gtfs_path, "trips.txt"))
    trips = trips.merge(routes, on="route_id", how="inner")

    matching_stop_times = matching_stop_times.merge(
        trips, on="trip_id", how="inner")

    # Custom fields
    matching_stop_times.loc[:, "scheduled_departure_day"] = yyyymmdd_format
    matching_stop_times.rename(
        columns={'departure_time': 'scheduled_departure_time'}, inplace=True)
    matching_stop_times.loc[:, "station_id"] = matching_stop_times[
        "stop_id"].str.extract("DUA(\d{7})", expand=False)
    matching_stop_times.loc[:, "train_num"] = matching_stop_times[
        "trip_id"].str.extract("^.{5}(\d{6})", expand=False)
    # Dynamo sort key (hash is station_id)
    matching_stop_times.loc[:, "day_train_num"] = matching_stop_times.apply(
        lambda x: "%s_%s" % (x["scheduled_departure_day"], x["train_num"]), axis=1)

    # Drop na and dups on indexes: station_id and day_train_num
    if isinstance(dropna_index, list):
        matching_stop_times = matching_stop_times.dropna(
            subset=dropna_index)
        matching_stop_times = matching_stop_times.drop_duplicates(
            subset=dropna_index)

    # Station filtering if asked
    if stop_filter:
        cond2 = matching_stop_times["stop_id"].isin(stop_filter)
        matching_stop_times = matching_stop_times[cond2]

    if station_filter:
        cond3 = matching_stop_times["station_id"].isin(station_filter)
        matching_stop_times = matching_stop_times[cond3]

    if df_format:
        return matching_stop_times
    else:
        json_list = json.loads(matching_stop_times.to_json(orient='records'))
        logger.info("There are %d scheduled departures on %s",
                    len(json_list), yyyymmdd_format)
        return json_list


def dynamo_save_stop_times_of_day(yyyymmdd_format, table_name):
    """
    Given a date, this function will save all trip-ids scheduled on transilien's network on this day in Dynamo's 'scheduled_departures' table.

    This function requires that gtfs files are present in data folder specified in settings module.

    :param yyyymmdd_format: date on yyyymmdd format
    :type yyyymmdd_format: string or int
    """
    if yyyymmdd_format == "all":
        items = get_departure_times_of_day_json_list(
            "all", df_format=False, dropna_index=["trip_id", "station_id"])
        dynamo_insert_batches(items, table_name)
    else:
        # Format it in a list
        if not isinstance(yyyymmdd_format, list):
            yyyymmdd_format = [str(yyyymmdd_format)]
        # Can take some time depending on write capacity
        for day in yyyymmdd_format:
            items = get_departure_times_of_day_json_list(str(day))
            dynamo_insert_batches(items, table_name)


def adapt_table_provision(func):
    """ Decorator to provision table before and after operation.
    """
    def wrapper(yyyymmdd_format, table_name):
        # Set provisioned_throughput
        try:
            dynamo_update_provisionned_capacity(
                read=shed_read, write=shed_write_on, table_name=table_name)
        except Exception as e:
            logger.warning("Could not change provisioned_throughput %s", e)

        # Wait for one minute till provisioned_throughput is updated
        time.sleep(60)
        # Actually perform operation
        func(yyyymmdd_format, table_name)
        # Reset provisioned_throughput to minimal writing
        dynamo_update_provisionned_capacity(
            read=shed_read, write=shed_write_off, table_name=table_name)
    return wrapper


@adapt_table_provision
def dynamo_save_stop_times_of_day_adapt_provision(yyyymmdd_format, table_name):
    """
    Given a date, this function will first update Dynamo's "scheduled_departures" table provisioned throughput to be able to save data in less than 30 minutes.

    Then it will save all trip-ids scheduled on transilien's network on this day in Dynamo's table.

    Finally it update table provisioned throughput to a lower value on write operations.

    :param yyyymmdd_format: date on yyyymmdd format
    :type yyyymmdd_format: string or int

    :param ignore_fail: default True. If you try to update provisioned throughput to existing values, it will raise an error. It is not a problem, so you might want to ignore this error.
    :type ignore_fail: boolean
    """
    dynamo_save_stop_times_of_day(yyyymmdd_format, table_name=table_name)


def build_trips_ext_df():
    """
    DEPRECATED

    Build trips extended dataframe out of:
    - trips
    - calendar
    Adds column:
    - train_num out of trip_id
    Clean:
    - remove NaN values
    """
    trips = pd.read_csv(path.join(gtfs_path, "trips.txt"))
    logger.debug("Found %d trips.", len(trips.index))
    calendar = pd.read_csv(path.join(gtfs_path, "calendar.txt"))

    trips["train_num"] = trips["trip_id"].str.extract(
        "^.{5}(\d{6})", expand=False)
    trips_ext = trips.merge(calendar, on="service_id", how="inner")

    # Clean missing fields
    # Block id is always NaN, and drop where calendar is not present
    del trips_ext['block_id']
    trips_ext.dropna(axis=0, how='any', inplace=True)
    return trips_ext


def build_stop_times_ext_df():
    """
    DEPRECATED

    Build stop times extended dataframe out of:
    - trips
    - calendar
    - stop_times
    - stops
    Adds column:
    - train_num out of trip_id
    - station_id out of stop_id
    Clean:
    - remove NaN values
    """
    trips = pd.read_csv(path.join(gtfs_path, "trips.txt"))
    calendar = pd.read_csv(path.join(gtfs_path, "calendar.txt"))
    stop_times = pd.read_csv(path.join(gtfs_path, "stop_times.txt"))
    stops = pd.read_csv(path.join(gtfs_path, "stops.txt"))

    stop_times_ext = stop_times.merge(trips, on="trip_id", how="inner")
    stop_times_ext = stop_times_ext.merge(
        calendar, on="service_id", how="inner")
    stop_times_ext = stop_times_ext.merge(stops, on="stop_id", how="inner")

    # Delete empty columns
    del stop_times_ext['block_id']
    del stop_times_ext['stop_headsign']
    del stop_times_ext['stop_desc']
    del stop_times_ext['zone_id']
    del stop_times_ext['stop_url']

    stop_times_ext["train_num"] = stop_times_ext[
        "trip_id"].str.extract("^.{5}(\d{6})", expand=False)
    stop_times_ext["station_id"] = stop_times_ext.stop_id.str.extract(
        "DUA(\d{7})", expand=False)

    # In some case we cannot extract station number
    stop_times_ext.dropna(axis=0, inplace=True)
    return stop_times_ext


def save_trips_extended_rdb(dryrun=False):
    """
    DEPRECATED

    Save trips_ext table
    """
    trips_ext = build_trips_ext_df()

    logger.debug("%d trips with calendar dates.", len(trips_ext.index))

    if not dryrun:
        connection = rdb_connection(db="postgres_alch")
        trips_ext.to_sql("trips_ext", connection, if_exists='replace',
                         index=False, index_label="trip_id", chunksize=1000)
    else:
        logger.info("Dryrun is True, database has not changed.")
    return trips_ext


def save_stop_times_extended_rdb(dryrun=False):
    """
    DEPRECATED

    Save stop_times_ext table
    """
    stop_times_ext = build_stop_times_ext_df()

    if not dryrun:
        connection = rdb_connection(db="postgres_alch")
        stop_times_ext.to_sql("stop_times_ext", connection, if_exists='replace',
                              index=True, index_label=None, chunksize=1000)
    else:
        logger.info("Dry run is True: database has not been modified.")
    return stop_times_ext


def save_all_schedule_tables_rdb():
    """
    DEPRECATED
    """
    save_trips_extended_rdb()
    save_stop_times_extended_rdb()
