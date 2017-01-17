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

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.utils_api_client import get_api_client
from src.utils_mongo import mongo_get_collection

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
    df_trains["request_date"] = datetime.now(
    ).strftime('%Y%m%dT%H%M%S')
    df_trains["station"] = station
    data_json = json.loads(df_trains.to_json(orient='records'))
    return data_json


def parse_and_save_station_response(response_text, station):
    print("Saving results for station %s" % station)
    try:
        data_json = xml_to_json_with_params(response_text, station)
    except:
        print("Cannot parse")
    try:
        collection = mongo_get_collection("departures")
        collection.insert_many(data_json)
    except:
        print("Cannot save in Mongo")


def operate_timer(station_list=station_ids, cycle_time_sec=1200, stop_time_sec=3600, max_per_minute=250):

    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    station_chunks = chunks(station_list, max_per_minute)
    begin_time = datetime.now()

    while (datetime.now() - begin_time).seconds < stop_time_sec:
        # Set cycle loop
        loop_begin_time = datetime.now()

        for station_chunk in station_chunks:
            chunk_begin_time = datetime.now()

            client = get_api_client()
            responses = client.request_stations(station_chunk)
            for response in responses:
                parse_and_save_station_response(response[0], response[1])

            time_passed = (datetime.now() - chunk_begin_time).seconds
            print(time_passed)
            # Max per minute
            if time_passed < 60:
                time.sleep(60 - time_passed)

        # Wait until beginning of next cycle
        time_passed = (datetime.now() - loop_begin_time).seconds
        print(time_passed)
        if time_passed < cycle_time_sec:
            time.sleep(cycle_time_sec - time_passed)


if __name__ == '__main__':
    operate_timer()
