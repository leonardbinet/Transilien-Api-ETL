from gevent import monkey
monkey.patch_all()

import os
from os import path, sys
import time
from datetime import datetime, timedelta
import ipdb
import requests
import json
import xmltodict
import pandas as pd
from gevent.pool import Pool

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.utils_api_client import get_api_client
from src.utils_mongo import mongo_get_collection

data_path = "../data/"

df_gares = pd.read_csv(os.path.join(
    data_path, "gares_transilien.csv"), sep=";")

station_ids = df_gares["Code UIC"].values


def xml_to_json_with_params(xml_string, station):
    mydict = xmltodict.parse(xml_string)
    trains = mydict["passages"]["train"]
    df_trains = pd.DataFrame(trains)
    df_trains["request_date"] = datetime.now(
    ).strftime('%Y%m%dT%H%M%S')
    df_trains["station"] = station
    data_json = json.loads(df_trains.to_json(orient='records'))
    return data_json


def extract_save_station(station):
    try:
        client = get_api_client()
        response = client.request_station(station=station, verbose=True)
    except:
        print("Cannot query station %s" % station)
    try:
        data_json = xml_to_json_with_params(response.text, station)
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

            pool = Pool(20)
            pool.map(extract_save_station, station_chunk)
            pool.join()
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
