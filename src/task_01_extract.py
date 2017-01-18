import asyncio
import os
from os import path, sys
import time
from datetime import datetime, timedelta
import ipdb
import requests
import json
import xmltodict
import pandas as pd
import requests
import zipfile
import io
import pytz

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.utils_api_client import get_api_client
from src.utils_mongo import mongo_get_collection, mongo_async_save_chunks

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))

data_path = os.path.join(BASE_DIR, "data")
df_gares = pd.read_csv(os.path.join(
    data_path, "gares_transilien.csv"), sep=";")

station_ids = df_gares["Code UIC"].values


def download_gtfs_files():
    data_url = 'https://ressources.data.sncf.com/explore/dataset/sncf-transilien-gtfs/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true'

    df_links_gtfs = pd.read_csv(data_url)

    for link in df_links_gtfs["file"].values:
        r = requests.get(link)
        z = zipfile.ZipFile(io.BytesIO(r.content))

        dir_path = path.join(BASE_DIR, "data", "sncf-transilien-gtfs")
        z.extractall(path=dir_path)


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
    print("Extraction of %d stations" % len(stations_list))
    client = get_api_client()
    responses = client.request_stations(stations_list)
    # Parse responses in JSON format
    print("Parsing")
    json_chunks = []
    for response in responses:
        try:
            json_chunks.append(xml_to_json_with_params(
                response[0], response[1]))
        except:
            continue
    # Save chunks in Mongo
    print("Saving of %d chunks of json data in Mongo" % len(json_chunks))
    mongo_async_save_chunks("departures", json_chunks)
    # Save chunks in other collection
    mongo_async_save_chunks("real_departures", json_chunks)


def operate_timer(station_list=station_ids, cycle_time_sec=1200, stop_time_sec=3600, max_per_minute=250):
    print("BEGINNING OPERATION WITH LIMIT OF %d SECONDS" % stop_time_sec)

    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    begin_time = datetime.now()

    while (datetime.now() - begin_time).seconds < stop_time_sec:
        # Set cycle loop
        loop_begin_time = datetime.now()
        print("BEGINNING CYCLE OF %d SECONDS" % cycle_time_sec)

        station_chunks = chunks(station_list, max_per_minute)

        for station_chunk in station_chunks:
            chunk_begin_time = datetime.now()
            extract_save_stations(station_chunk)

            time_passed = (datetime.now() - chunk_begin_time).seconds
            print("Time spent: %d seconds" % int(time_passed))

            # Max per minute: so have to wait
            if time_passed < 60:
                time.sleep(60 - time_passed)

        # Wait until beginning of next cycle
        time_passed = (datetime.now() - loop_begin_time).seconds
        print("Time spent on cycle: %d seconds" % int(time_passed))
        if time_passed < cycle_time_sec:
            time_to_wait = cycle_time_sec - time_passed
            print("Waiting %d seconds till next cycle." % time_to_wait)
            time.sleep(time_to_wait)

        # Information about general timing
        time_from_begin = (datetime.now() - begin_time).seconds
        print("Time spent from beginning: %d seconds. (stop at %d seconds)" %
              (time_from_begin, stop_time_sec))


if __name__ == '__main__':
    # By default, run for one hour (minus 100 sec), every 2 minutes
    # max 300 queries per sec
    operate_timer(station_list=station_ids, cycle_time_sec=120,
                  stop_time_sec=3500, max_per_minute=300)
