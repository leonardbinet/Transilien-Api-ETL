import asyncio
import os
from os import path, sys
import time
from datetime import datetime, timedelta
import ipdb
import json
import xmltodict
import pandas as pd
import pytz
import copy
import logging

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.utils_api_client import get_api_client
from src.utils_mongo import mongo_get_collection, mongo_async_save_chunks, mongo_async_upsert_chunks

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))
data_path = os.path.join(BASE_DIR, "data")

# Logging configuration
logging_file_path = os.path.join(BASE_DIR, "..", "logs", "task01.log")
logging.basicConfig(format='%(levelname)s:%(message)s',
                    filename=logging_file_path, level=logging.INFO)


def get_station_ids(id_format="UIC"):
    df_gares = pd.read_csv(os.path.join(
        data_path, "gares_transilien.csv"), sep=";")
    station_ids = df_gares["Code UIC"].values

    if id_format == "UIC":
        return station_ids
    elif id_format == "UIC7":
        station_ids_uic7 = list(map(lambda x: str(x)[:-1], station_ids))
        return station_ids_uic7
    else:
        return False


def xml_to_json_with_params(xml_string, station):
    mydict = xmltodict.parse(xml_string)
    trains = mydict["passages"]["train"]
    df_trains = pd.DataFrame(trains)
    df_trains["date"] = df_trains.date.apply(lambda x: x["#text"])
    # Save with Paris timezone (if server is abroad)
    paris_tz = pytz.timezone('Europe/Paris')
    datetime_paris = paris_tz.localize(datetime.now())
    df_trains["request_day"] = datetime_paris.strftime('%Y%m%d')
    df_trains["request_time"] = datetime_paris.strftime('%H:%M:%S')
    df_trains["station"] = station
    data_json = json.loads(df_trains.to_json(orient='records'))
    return data_json


def extract_save_stations(stations_list):
    # Extract from API
    logging.info("Extraction of %d stations" % len(stations_list))
    client = get_api_client()
    responses = client.request_stations(stations_list)
    # Parse responses in JSON format
    logging.info("Parsing")
    json_chunks = []
    for response in responses:
        try:
            xml_string = response[0]
            station = response[1]
            json_chunks.append(xml_to_json_with_params(
                xml_string, station))
        except:
            continue
    # Save chunks in Mongo
    # Make a deep copy, because mongo will add _ids to json_chunks
    json_chunks_2 = copy.deepcopy(json_chunks)
    item_list = [item for sublist in json_chunks_2 for item in sublist]

    logging.info("Saving of %d chunks of json data (total of %d items) in Mongo departures collection" %
                 (len(json_chunks), len(item_list)))
    mongo_async_save_chunks("departures", json_chunks)
    # Save items in other collection
    # flatten chunks: -> list of elements to upsert

    index_fields = ["request_day", "station", "num"]
    logging.info("Upsert of %d items of json data in Mongo real_departures collection" %
                 len(item_list))
    mongo_async_upsert_chunks("real_departures", item_list, index_fields)


def operate_timer(station_filter=False, cycle_time_sec=1200, stop_time_sec=3600, max_per_minute=250):

    if not station_filter:
        station_list = get_station_ids()
    else:
        station_list = station_filter

    logging.info("BEGINNING OPERATION WITH LIMIT OF %d SECONDS" %
                 stop_time_sec)
    logging.info("MAX NUMBER OF QUERY PER MINUTE TO API: %d" % max_per_minute)

    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    begin_time = datetime.now()

    while (datetime.now() - begin_time).seconds < stop_time_sec:
        # Set cycle loop
        loop_begin_time = datetime.now()
        logging.info("BEGINNING CYCLE OF %d SECONDS" % cycle_time_sec)

        station_chunks = chunks(station_list, max_per_minute)

        for station_chunk in station_chunks:
            chunk_begin_time = datetime.now()
            extract_save_stations(station_chunk)

            time_passed = (datetime.now() - chunk_begin_time).seconds
            logging.info("Time spent: %d seconds" % int(time_passed))

            # Max per minute: so have to wait
            if time_passed < 60:
                time.sleep(60 - time_passed)
            else:
                logging.warning(
                    "Chunk time took more than one minute: %d seconds" % time_passed)

        # Wait until beginning of next cycle
        time_passed = (datetime.now() - loop_begin_time).seconds
        logging.info("Time spent on cycle: %d seconds" % int(time_passed))
        if time_passed < cycle_time_sec:
            time_to_wait = cycle_time_sec - time_passed
            logging.info("Waiting %d seconds till next cycle." % time_to_wait)
            time.sleep(time_to_wait)
        else:
            logging.warning(
                "Cycle time took more than expected: %d seconds" % time_passed)

        # Information about general timing
        time_from_begin = (datetime.now() - begin_time).seconds
        logging.info("Time spent from beginning: %d seconds. (stop at %d seconds)" %
                     (time_from_begin, stop_time_sec))


if __name__ == '__main__':
    # By default, run for one hour (minus 100 sec), every 2 minutes
    # max 300 queries per sec
    operate_timer(station_filter=False, cycle_time_sec=120,
                  stop_time_sec=3500, max_per_minute=300)
