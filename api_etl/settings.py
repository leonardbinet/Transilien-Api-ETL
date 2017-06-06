"""
Module regrouping all project's settings.
"""

import os
from os import path

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))

logs_path = os.path.join(BASE_DIR, "..", "logs")


# ##### S3 BUCKETS #####
s3_prefix = "transilien-project"
s3_buckets = {
    "etl-logs": "%s.etl-logs" % s3_prefix,
    "gtfs-files": "%s.gtfs-files" % s3_prefix,
    "training-sets": "%s.training-sets" % s3_prefix,
}

# ##### DATA PATH #####
data_path = os.path.join(BASE_DIR, "data")
gtfs_path = os.path.join(data_path, "gtfs-lines-last")

gtfs_csv_url = 'https://ressources.data.sncf.com/explore/dataset/sncf-transilien-gtfs/'\
    + 'download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true'

# Stations files paths
responding_stations_path = path.join(data_path, "responding_stations.csv")
top_stations_path = path.join(data_path, "most_used_stations.csv")
scheduled_stations_path = path.join(
    data_path, "scheduled_station_20170215.csv")
all_stations_path = path.join(data_path, "all_stations.csv")
stations_per_line_path = path.join(data_path, "sncf-lignes-par-gares-idf.csv")

# ##### DATABASES #####

# MONGO
# Mongo DB collections:
mongo_realtime_unique = {
    "name": "real_departures_2",
    "unique_index": ["request_day", "station", "train_num"]
}

mongo_realtime_all = {
    "name": "departures"
}

# DYNAMO
# Dynamo DB tables:
dynamo_realtime = {
    "name": "real_departures_2",
    "provisioned_throughput": {
        "read": 50,
        "write": 80
    }
}

# Deprecated
# Dynamo was previously used for saving schedule: no more, now in realtional
# database.
dynamo_schedule = {
    "name": "scheduled_departures",
    "provisioned_throughput": {
        "on": {
            "read": 100,
            "write": 100
        },
        "off": {
            "read": 100,
            "write": 1
        }
    }
}
