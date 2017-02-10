import asyncio
import os
import time
from datetime import datetime, timedelta
import json
import xmltodict
import pandas as pd
import copy
import logging
import numpy as np

from api_transilien_manager.utils_misc import get_paris_local_datetime_now, compute_delay, chunks
from api_transilien_manager.utils_api_client import get_api_client
from api_transilien_manager.utils_mongo import mongo_get_collection, mongo_async_save_chunks, mongo_async_upsert_items
from api_transilien_manager.utils_dynamo import dynamo_insert_batches
from api_transilien_manager.settings import data_path, col_real_dep_unique, responding_stations_path, all_stations_path, dynamo_table, top_stations_path

logger = logging.getLogger(__name__)

# To avoid some pandas warnings
pd.options.mode.chained_assignment = None


def get_station_ids(id_format="UIC", stations="responding"):
    # Choose which file we load, depending on stations parameter: if all
    if stations == "all":
        df_gares = pd.read_csv(all_stations_path, sep=";")
        station_ids = list(df_gares["Code UIC"].values)
        station_ids = list(map(lambda x: str(x), station_ids))

    elif stations == "responding":
        station_ids = np.genfromtxt(
            responding_stations_path, delimiter=',', dtype=str)

    elif stations == "top":
        station_ids = np.genfromtxt(
            top_stations_path, delimiter=',', dtype=str)
    else:
        raise ValueError(
            "stations parameter should be either 'all', 'top' or 'responding'")

    # Choose in which format we want it: either UIC: 8 digits, or UIC7: 7
    # digits
    if id_format == "UIC":
        return list(station_ids)
    elif id_format == "UIC7":
        station_ids_uic7 = list(map(lambda x: str(x)[:-1], station_ids))
        return station_ids_uic7
    else:
        logger.error("id_format must be either UIC or UIC7")
        raise ValueError("Incorrect id_format provided.")


def api_date_to_day_time_corrected(api_date, time_or_day):
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


def xml_to_json_item_list(xml_string, station):
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
    df_trains.loc[:, "station"] = station
    df_trains.rename(columns={'num': 'train_num'}, inplace=True)
    # Data freshness is time in seconds between request time and
    # expected_passage_time: lower is better
    df_trains.loc[:, "data_freshness"] = df_trains.apply(lambda x: abs(
        compute_delay(x["request_time"], x["expected_passage_time"])), axis=1)
    # Our hash key for dynamodb
    df_trains.loc[:, "day_station"] = df_trains.apply(
        lambda x: "%s_%s" % (x["expected_passage_day"], x["station"]), axis=1)

    data_json = json.loads(df_trains.to_json(orient='records'))
    return data_json


def extract_save_stations(stations_list, dynamo_unique=True, mongo_unique=True, mongo_all=True):
    """
    Use col_real_dep_unique defined in settings to know in which collection to
    save data.
    """
    # Extract from API
    logger.info("Extraction of %d stations" % len(stations_list))
    client = get_api_client()
    responses = client.request_stations(stations_list)

    # Parse responses in JSON format
    logger.info("Parsing")
    json_chunks = []
    for response in responses:
        xml_string = response[0]
        station = response[1]
        try:
            json_chunks.append(xml_to_json_item_list(
                xml_string, station))
        except Exception as e:
            logger.debug("Cannot parse station %s" % station)
            continue

    # Make deep copies, because mongo will add _ids to json_chunks
    # For mongo 'real_departures'
    json_chunks_2 = copy.deepcopy(json_chunks)
    items_list2 = [item for sublist in json_chunks_2 for item in sublist]

    # For dynamo 'real_departures'
    json_chunks_3 = copy.deepcopy(json_chunks)
    items_list3 = [item for sublist in json_chunks_3 for item in sublist]

    if mongo_all:
        # Save items in collection without compound primary key
        logger.info("Saving of %d chunks of json data (total of %d items) in Mongo departures collection" %
                    (len(json_chunks), len(items_list2)))
        mongo_async_save_chunks("departures", json_chunks)

    if mongo_unique:
        # Save items in collection with compound primary key
        index_fields = ["request_day", "station", "train_num"]
        logger.info("Upsert of %d items of json data in Mongo %s collection" %
                    (len(items_list2), col_real_dep_unique))
        mongo_async_upsert_items(
            col_real_dep_unique, items_list2, index_fields)

    if dynamo_unique:
        logger.info("Upsert of %d items of json data in dynamo %s table" %
                    (len(items_list3), dynamo_table))
        dynamo_insert_batches(items_list3, table_name=dynamo_table)


def operate_one_cycle(station_filter=False, max_per_minute=300):

    if not station_filter:
        station_list = get_station_ids()
    else:
        station_list = station_filter

    # station_chunks = chunks(station_list, max_per_minute)
    # split stations in two of same size
    station_chunks = [station_list[i::2] for i in range(2)]

    for station_chunk in station_chunks:
        chunk_begin_time = datetime.now()
        extract_save_stations(station_chunk)

        time_passed = (datetime.now() - chunk_begin_time).seconds
        logger.info("Time spent: %d seconds" % int(time_passed))

        # Max per minute: so have to wait
        if time_passed < 60:
            time.sleep(60 - time_passed)
        else:
            logger.warning(
                "Chunk time took more than one minute: %d seconds" % time_passed)


def operate_multiple_cycles(station_filter=False, cycle_time_sec=1200, stop_time_sec=3600, max_per_minute=300):

    logger.info("BEGINNING OPERATION WITH LIMIT OF %d SECONDS" %
                stop_time_sec)
    logger.info("MAX NUMBER OF QUERY PER MINUTE TO API: %d" % max_per_minute)
    begin_time = datetime.now()

    while (datetime.now() - begin_time).seconds < stop_time_sec:
        # Set cycle loop
        loop_begin_time = datetime.now()
        logger.info("BEGINNING CYCLE OF %d SECONDS" % cycle_time_sec)

        operate_one_cycle(station_filter=station_filter,
                          max_per_minute=max_per_minute)

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
