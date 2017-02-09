import os
from os import path

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))

data_path = os.path.join(BASE_DIR, "data")
gtfs_path = os.path.join(data_path, "gtfs-lines-last")

gtfs_csv_url = 'https://ressources.data.sncf.com/explore/dataset/sncf-transilien-gtfs/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true'


sqlite_path = os.path.join(BASE_DIR, "schedules.db")

logs_path = os.path.join(BASE_DIR, "..", "logs")


# Mongo DB collections:
col_real_dep_unique = "real_departures_2"

# Responding stations csv file location
responding_stations_path = path.join(data_path, "responding_stations.csv")
top_stations_path = path.join(data_path, "most_used_stations.csv")

# All stations csv file location
all_stations_path = path.join(data_path, "gares_transilien.csv")

# Dynamo DB tables:
dynamo_table = "real_departures"
