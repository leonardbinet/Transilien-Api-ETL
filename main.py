from configuration import API_USER, API_PASSWORD, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST
import defusedxml.ElementTree as ET

import os
import requests
import json
import xmltodict
import pandas as pd

example_url = "http://api.transilien.com/gare/87393009/depart/"

object_ids = []
core_url = "http://api.transilien.com/"
responses = []

for object_id in object_ids:
    url = os.path.join(core_url, "gare", object_id, "depart")
    responses.append(requests.get(url, auth=(API_USER, API_PASSWORD)))

response1 = requests.get(example_url, auth=(API_USER, API_PASSWORD))

mydict = xmltodict.parse(response1.text)
#json_data = json.loads(mydict)

trains = mydict["passages"]["train"]
df_trains = pd.DataFrame(trains)

"""
import mysql.connector
cnx = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                              host=MYSQL_HOST,
                              database='api_transilien')
"""
