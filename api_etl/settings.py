"""
Module regrouping all project's settings.
"""

import os
from os import path

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))

logs_path = os.path.join(BASE_DIR, "..", "logs")


#### DATA PATH #####
data_path = os.path.join(BASE_DIR, "data")
gtfs_path = os.path.join(data_path, "gtfs-lines-last")

gtfs_csv_url = 'https://ressources.data.sncf.com/explore/dataset/sncf-transilien-gtfs/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true'

# Stations files paths
# test.tofile("data/all_stations.csv",sep=",",format="%s")
# np.genfromtxt("etc.csv", delimiter=",", dtype=str)
responding_stations_path = path.join(data_path, "responding_stations.csv")
top_stations_path = path.join(data_path, "most_used_stations.csv")
scheduled_stations_path = path.join(
    data_path, "scheduled_station_20170215.csv")
all_stations_path = path.join(data_path, "all_stations.csv")


#### DATABASES ####

# Sqlite
sqlite_path = os.path.join(BASE_DIR, "schedules.db")

# Mongo DB collections:
col_real_dep_unique = "real_departures_2"

# Dynamo DB tables:
dynamo_real_dep = "real_departures_2"
dynamo_sched_dep = "scheduled_departures"
dynamo_sched_dep_all = "scheduled_departures_all"

# Dynamo provisioned_throughput for tables
real_read = 50
real_write = 80
# 'on' is for when we update, 'off' rest of the time
shed_read = 100
shed_write_off = 1
shed_write_on = 100
