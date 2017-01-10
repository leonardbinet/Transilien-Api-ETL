import os
import time
from datetime import datetime, timedelta
import ipdb
import requests
import json
import xmltodict
import pandas as pd
# import mysql.connector
from pymongo import MongoClient
import defusedxml.ElementTree as ET


API_USER = os.environ["API_USER"]
API_PASSWORD = os.environ["API_PASSWORD"]
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]
MYSQL_DB_NAME = os.environ["MYSQL_DB_NAME"]
MYSQL_HOST = os.environ["MYSQL_HOST"]
MONGO_HOST = os.environ["MONGO_HOST"]
MONGO_USER = os.environ["MONGO_USER"]
MONGO_DB_NAME = os.environ["MONGO_DB_NAME"]
MONGO_PASSWORD = os.environ["MONGO_PASSWORD"]


# example_url = "http://api.transilien.com/gare/87393009/depart/"


def extract_save_station(station, api_user, api_password, collection):
    print("Extraction for station %s" % station)
    core_url = "http://api.transilien.com/"
    url = os.path.join(core_url, "gare", str(station), "depart")
    response = requests.get(url, auth=(API_USER, API_PASSWORD))
    print(response.text)
    mydict = xmltodict.parse(response.text)
    trains = mydict["passages"]["train"]
    df_trains = pd.DataFrame(trains)
    df_trains["request_date"] = datetime.now(
    ).strftime('%Y%m%dT%H%M%S')
    df_trains["station"] = station
    data_json = json.loads(df_trains.to_json(orient='records'))
    print(data_json)
    print(df_trains.to_json(orient='records'))
    collection.insert_many(data_json)

# Connect to mongodb:
c = MongoClient(host=MONGO_HOST)
db = c[MONGO_DB_NAME]
db.authenticate(MONGO_USER, MONGO_PASSWORD)
collection = db["departures"]
# db.command("collstats","departures")

# Bug d'import sur pythonanywhere
df_gares = pd.read_csv("Data/gares_transilien.csv", sep=";")
station_ids = df_gares["Code UIC"].values

begin_time = datetime.now()


# Programm run for an hour before new instance
while (datetime.now() - begin_time).seconds < 3600:

    loop_begin_time = datetime.now()
    # Every minute, computes half of stations, then other half
    for station in station_ids[:250]:
        try:
            extract_save_station(station, API_USER, API_PASSWORD, collection)
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
            extract_save_station(station, API_USER, API_PASSWORD, collection)
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
