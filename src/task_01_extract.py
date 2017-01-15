import os
import time
from datetime import datetime, timedelta
import ipdb
import requests
import json
import xmltodict
import pandas as pd
from multiprocessing import Pool

# import mysql.connector
# import defusedxml.ElementTree as ET
from src.utils_api_client import get_api_client
from src.utils_mongo import mongo_get_collection


# Bug d'import sur pythonanywhere
df_gares = pd.read_csv("data/gares_transilien.csv", sep=";")
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
    # Define blocks
    # Set stop time
    begin_time = datetime.now()
    while (datetime.now() - begin_time).seconds < stop_time_sec:
        # Set cycle loop
        loop_begin_time = datetime.now()

        pool = Pool(processes=10)
        pool.map(extract_save_station, station_list[:max_per_minute])
        pool.close()
        pool.join()

        time_passed = (datetime.now() - loop_begin_time).seconds
        print(time_passed)
        # Max per minute
        if time_passed < 60:
            time.sleep(60 - time_passed)

        pool = Pool(processes=10)
        pool.map(extract_save_station, station_list[max_per_minute:])
        pool.close()
        pool.join()

        # Wait until beginning of next cycle
        time_passed = (datetime.now() - loop_begin_time).seconds
        print(time_passed)
        if time_passed < cycle_time_sec:
            time.sleep(cycle_time_sec - time_passed)


if __name__ == '__main__':
    begin_time = datetime.now()
    # Programm run for an hour before new instance
    while (datetime.now() - begin_time).seconds < 3600:

        loop_begin_time = datetime.now()
        # Every minute, computes half of stations, then other half
        for station in station_ids[:250]:
            try:
                extract_save_station(station)
            except Exception as e:
                # Horrible coding, but work for now, so that a failing request does
                # not stop the whole process if no result
                print(e)
                with open("log.txt", "a") as myfile:
                    myfile.write(str(station) + "\n")
                continue

        time_passed = (datetime.now() - loop_begin_time).seconds
        print(time_passed)
        if time_passed < 60:
            time.sleep(60 - time_passed)

        for station in station_ids[250:]:
            try:
                extract_save_station(station)
            except Exception as e:
                # Horrible coding, but work for now, so that a failing request does
                # not stop the whole process if no result
                with open("log.txt", "a") as myfile:
                    myfile.write(str(station) + "\n")
                continue

        # Cycles of 120 seconds for whole process
        time_passed = (datetime.now() - loop_begin_time).seconds
        print(time_passed)
        if time_passed < 120:
            time.sleep(120 - time_passed)
