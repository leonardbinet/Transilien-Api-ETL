from configuration import API_USER, API_PASSWORD, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB_NAME, MYSQL_HOST, MONGO_HOST, MONGO_USER, MONGO_DB_NAME, MONGO_PASSWORD
import defusedxml.ElementTree as ET

import os
import time
from datetime import datetime
import ipdb
import requests
import json
import xmltodict
import pandas as pd
# import mysql.connector
from pymongo import MongoClient


# example_url = "http://api.transilien.com/gare/87393009/depart/"

def extract_save_station(station, api_user, api_password, collection):
    print("Extraction for station %s" % station)
    core_url = "http://api.transilien.com/"
    url = os.path.join(core_url, "gare", str(station), "depart")
    response = requests.get(url, auth=(API_USER, API_PASSWORD))
    mydict = xmltodict.parse(response.text)
    trains = mydict["passages"]["train"]
    df_trains = pd.DataFrame(trains)
    df_trains["request_date"] = datetime.now(
    ).strftime('%Y%m%dT%H%M%S')
    df_trains["station"] = station
    data_json = json.loads(df_trains.to_json(orient='records'))
    collection.insert_many(data_json)

# Connect to mongodb:
c = MongoClient(host=MONGO_HOST)
db = c[MONGO_DB_NAME]
db.authenticate(MONGO_USER, MONGO_PASSWORD)
collection = db["departures"]

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
                myfile.write(str(station))
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
                myfile.write(str(station))
            continue

    # Cycles of 120 seconds for whole process
    time_passed = (datetime.now() - loop_begin_time).seconds
    print(time_passed)
    if time_passed < 120:
        time.sleep(120 - time_passed)


"""
# FOR MYSQL CLIENT

cnx = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                              host=MYSQL_HOST,
                              database=MYSQL_DB_NAME)
cursor = cnx.cursor()

add_departure = ("INSERT INTO departures "
                 "(station, request_date, date, num, miss, term, etat) "
                 "VALUES (%s, %s, %s, %s, %s, %s, %s)")


for df in df_list:
    data_departure = {}
    cursor.execute(add_departure, data_departure)
    # emp_no = cursor.lastrowid, if we want last id created
"""
