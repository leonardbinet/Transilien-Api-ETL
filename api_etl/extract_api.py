"""
Core module using other modules to:
- extract data from the Transilien's API (use utils_api_client)
- update it with GTFS data (use query_schedule, if data was previously saved with extract_schedule)
- save it in databases, Mongo or/and Dynamo (use utils_dynamo, utils_mongo)
"""

import time
from datetime import datetime
import json
import xmltodict
import pandas as pd
import copy
import logging

from api_etl.utils_misc import get_paris_local_datetime_now, compute_delay, chunks, get_station_ids, api_date_to_day_time_corrected
from api_etl.utils_api_client import ApiClient
from api_etl.utils_mongo import mongo_get_collection, mongo_async_save_chunks, mongo_async_upsert_items
from api_etl.utils_dynamo import dynamo_insert_batches

from api_etl.settings import data_path, col_real_dep_unique, responding_stations_path, all_stations_path, dynamo_real_dep, top_stations_path, scheduled_stations_path

from api_etl.query_schedule import dynamo_extend_items_with_schedule

logger = logging.getLogger(__name__)

# To avoid some pandas warnings
pd.options.mode.chained_assignment = None


def xml_to_json_item_list(xml_string, station, df_format=False):
    """
    This function transforms transilien's API XML answers into a list of objects, in a valid format.
    It might be useful to use a class to define object structure.

    :param xml_string: XML string you want to transform
    :type xml_string: string

    :param station: station id that was queried. This is necessary to add a station field in output. This must be a 8 digits string or integer. You can find the stations ids on SNCF website. https://ressources.data.sncf.com/explore/dataset/referentiel-gares-voyageurs/
    :type station: string/integer

    :param df_format: default False, set True so that the function returns a pandas dataframe instead of a list of dictionnaries.
    :type df_format: boolean

    :rtype: list of dictionnaries of strings only, this is json-serializable so it can easily be inserted in Mongo or Dynamo databases.
    """
    # Save with Paris timezone (if server is abroad)
    datetime_paris = get_paris_local_datetime_now()

    mydict = xmltodict.parse(xml_string)
    trains = mydict["passages"]["train"]
    df_trains = pd.DataFrame(trains)

    # Add custom fields
    df_trains.loc[:, "date"] = df_trains.date.apply(lambda x: x["#text"])
    df_trains.loc[:, "expected_passage_day"] = df_trains[
        "date"].apply(lambda x: api_date_to_day_time_corrected(x, "day"))
    df_trains.loc[:, "expected_passage_time"] = df_trains[
        "date"].apply(lambda x: api_date_to_day_time_corrected(x, "time"))
    df_trains.loc[:, "request_day"] = datetime_paris.strftime('%Y%m%d')
    df_trains.loc[:, "request_time"] = datetime_paris.strftime('%H:%M:%S')
    df_trains.loc[:, "station_8d"] = str(station)
    df_trains.loc[:, "station_id"] = str(station)[:-1]
    df_trains.rename(columns={'num': 'train_num'}, inplace=True)
    # Data freshness is time in seconds between request time and
    # expected_passage_time: lower is better
    df_trains.loc[:, "data_freshness"] = df_trains.apply(lambda x: abs(
        compute_delay(x["request_time"], x["expected_passage_time"])), axis=1)

    # Hash key for dynamodb: formerly: day_station, now, station (7 digits)
    # Sort key for dynamodb: formerly: train_num, now, day_train_num
    df_trains.loc[:, "day_train_num"] = df_trains.apply(
        lambda x: "%s_%s" % (x["expected_passage_day"], x["train_num"]), axis=1)

    if df_format:
        return df_trains

    data_json = json.loads(df_trains.to_json(orient='records'))
    return data_json


def extract_stations(stations_list):
    """
    This function will query transilien's API asking for expected trains passages in near future on stations defined as paramater. Note that stations id required by api are in 8 digits format.

    This function will return clean data (json serializable), ready to be inserted in Mongo or Dynamo databases.

    :param stations_list: these are the stations for which the transilien's api will be queried. List of stations of length 8.
    :type stations_list: list of str/int

    :rtype: list of dictionnaries (of strings)
    """
    # Extract from API
    logger.info("Extraction of %d stations" % len(stations_list))
    client = ApiClient()
    responses = client.request_stations(stations_list)

    # Parse responses in JSON format
    logger.info("Parsing")
    items_list = []
    for response in responses:
        xml_string = response[0]
        station = response[1]
        try:
            items_list += xml_to_json_item_list(
                xml_string, station)
        except Exception as e:
            logger.debug("Cannot parse station %s" % station)
            continue
    return items_list


def save_stations(items_list, dynamo_unique, mongo_unique, mongo_all):
    """
    This function will save in the specified databases, with uniqueness or not, the items provided as parameter.

    :param items_list: the items you want to save. They must be in relevant format, and contain fields that are used as primary fields.
    :type item_list: list of dictionnaries of strings (json serializable)

    :param dynamo_unique: save items in dynamo table that save unique passages
    :type dynamo_unique: boolean

    :param mongo_unique: save items in dynamo table that save unique passages
    :type mongo_unique: boolean

    :param mongo_all: save items in dynamo table that save all passages
    :type mongo_all: boolean
    """
    # Make deep copies, because mongo will add _ids
    items_list2 = copy.deepcopy(items_list)
    items_list3 = copy.deepcopy(items_list)

    if dynamo_unique:
        logger.info("Upsert of %d items of json data in dynamo %s table" %
                    (len(items_list), dynamo_real_dep))
        dynamo_insert_batches(items_list, table_name=dynamo_real_dep)

    if mongo_all:
        # Save items in collection without compound primary key
        logger.info("Saving  %d items in Mongo departures collection" %
                    len(items_list2))
        # Save in chunks of 100
        chunks = [items_list2[i:i + 100]
                  for i in range(0, len(items_list2), 100)]
        mongo_async_save_chunks("departures", chunks)

    if mongo_unique:
        # Save items in collection with compound primary key
        index_fields = ["request_day", "station", "train_num"]
        logger.info("Upsert of %d items of json data in Mongo %s collection" %
                    (len(items_list3), col_real_dep_unique))
        mongo_async_upsert_items(
            col_real_dep_unique, items_list3, index_fields)


def extract_save_stations(stations_list, dynamo_unique, mongo_unique, mongo_all):
    """
    This function performs the following operations:
    - query transilien's api for the requested stations
    - transform xml answers into json-serializable items
    - update items with schedule information
    - save it in requested databases

    :param stations_list: these are the stations for which the transilien's api will be queried. List of stations of length 8.
    :type stations_list: list of str/int

    :param dynamo_unique: save items in dynamo table that save unique passages
    :type dynamo_unique: boolean

    :param mongo_unique: save items in dynamo table that save unique passages
    :type mongo_unique: boolean

    :param mongo_all: save items in dynamo table that save all passages
    :type mongo_all: boolean
    """
    # Extract
    items_list = extract_stations(stations_list)
    # Update
    items_list = dynamo_extend_items_with_schedule(items_list)
    # Save
    save_stations(items_list, dynamo_unique=dynamo_unique,
                  mongo_unique=mongo_unique, mongo_all=mongo_all)


def operate_one_cycle(station_filter=False, dynamo_unique=True, mongo_unique=False, mongo_all=False):
    """
    This function performs the extract_save_stations operation in two steps. First half of stations, then second half of stations. This is made so that we do not exceed transilien's api max requests per minute (350/min).

    :param station_filter: default to False. If no filter, the function will take all stations provided by the get_station_ids function. If set to list, these are the stations for which the transilien's api will be queried. List of stations of length 8.
    :type station_filter: list of str/int OR False

    :param dynamo_unique: save items in dynamo table that save unique passages
    :type dynamo_unique: boolean

    :param mongo_unique: save items in dynamo table that save unique passages
    :type mongo_unique: boolean

    :param mongo_all: save items in dynamo table that save all passages
    :type mongo_all: boolean
    """
    if not station_filter:
        station_list = get_station_ids("all")
    else:
        station_list = station_filter

    # station_chunks = chunks(station_list, max_per_minute)
    # split stations in two of same size
    station_chunks = [station_list[i::2] for i in range(2)]

    for station_chunk in station_chunks:
        chunk_begin_time = datetime.now()
        extract_save_stations(
            station_chunk,
            mongo_all=mongo_all,
            dynamo_unique=dynamo_unique,
            mongo_unique=mongo_unique
        )

        time_passed = (datetime.now() - chunk_begin_time).seconds
        logger.info("Time spent: %d seconds" % int(time_passed))

        # Max per minute: so have to wait
        if time_passed < 60:
            time.sleep(60 - time_passed)
        else:
            logger.warning(
                "Chunk time took more than one minute: %d seconds" % time_passed)


def operate_multiple_cycles(station_filter=False, cycle_time_sec=1200, stop_time_sec=3600):
    """
    Deprecated: a cron job is scheduled to run operate_one_cycle every 'n' minutes.

    This function performs the operate_one_cycle operation every 'cycle_time_sec' seconds (default 10 minutes), during the time defined as 'stop_time_sec' (default 60 minutes).

    :param station_filter: default to False. If no filter, the function will take all stations provided by the get_station_ids function. If set to list, these are the stations for which the transilien's api will be queried. List of stations of length 8.
    :type station_filter: list of str/int OR False

    :param cycle_time_sec: number of seconds between each cycle beginning.
    :type cycle_time_sec: int

    :param stop_time_sec: number of seconds until a new cycle cannot begin.
    :type stop_time_sec: int
    """

    logger.info("BEGINNING OPERATION WITH LIMIT OF %d SECONDS" %
                stop_time_sec)
    begin_time = datetime.now()

    while (datetime.now() - begin_time).seconds < stop_time_sec:
        # Set cycle loop
        loop_begin_time = datetime.now()
        logger.info("BEGINNING CYCLE OF %d SECONDS" % cycle_time_sec)

        operate_one_cycle(station_filter=station_filter)

        # Wait until beginning of next cycle
        time_passed = (datetime.now() - loop_begin_time).seconds
        logger.info("Time spent on cycle: %d seconds" % int(time_passed))
        if time_passed < cycle_time_sec:
            time_to_wait = cycle_time_sec - time_passed
            logger.info("Waiting %d seconds till next cycle." % time_to_wait)
            time.sleep(time_to_wait)
        else:
            logger.warning(
                "Cycle time took more than expected: %d seconds" % time_passed)

        # Information about general timing
        time_from_begin = (datetime.now() - begin_time).seconds
        logger.info("Time spent from beginning: %d seconds. (stop at %d seconds)" %
                    (time_from_begin, stop_time_sec))
