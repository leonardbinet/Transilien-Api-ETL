"""
Module regrouping all project's settings.
"""

from os import path

__BASE_DIR__ = path.dirname(path.dirname(path.abspath(__file__)))

__LOGS_PATH__ = path.join(__BASE_DIR__, "..", "logs")


# ##### S3 BUCKETS #####
__S3_PREFIX__ = "transilien-project"
__S3_BUCKETS__ = {
    "etl-logs": "%s.etl-logs" % __S3_PREFIX__,
    "gtfs-files": "%s.gtfs-files" % __S3_PREFIX__,
    "training-sets": "%s.training-sets" % __S3_PREFIX__,
}

# ##### DATA PATH #####
__DATA_PATH__ = path.join(__BASE_DIR__, "data")
__GTFS_PATH__ = path.join(__DATA_PATH__, "gtfs-lines-last")

__GTFS_CSV_URL__ = 'https://ressources.data.sncf.com/explore/dataset/sncf-transilien-gtfs/' \
                   + 'download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true'

# Stations files paths
__RESPONDING_STATIONS_PATH__ = path.join(__DATA_PATH__, "responding_stations.csv")
__TOP_STATIONS_PATH__ = path.join(__DATA_PATH__, "most_used_stations.csv")
__SCHEDULED_STATIONS_PATH__ = path.join(__DATA_PATH__, "scheduled_station_20170215.csv")
__ALL_STATIONS_PATH__ = path.join(__DATA_PATH__, "all_stations.csv")
__STATIONS_PER_LINE_PATH__ = path.join(__DATA_PATH__, "sncf-lignes-par-gares-idf.csv")

# ##### DATABASES #####

# DYNAMO
# Dynamo DB tables:
__DYNAMO_REALTIME__ = {
    "name": "real_departures_2",
    "provisioned_throughput": {
        "read": 50,
        "write": 80
    }
}

# LOGGING

__LOGGING_CONFIG__ = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '[%(levelname)s] %(name)s: %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'defaultStream': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'detailedStream': {
            'level': 'DEBUG',
            'formatter': 'detailed',
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        '': {
            'handlers': ['defaultStream'],
            'level': 'INFO',
            'propagate': False
        },
        'api_etl': {
            'handlers': ['defaultStream'],
            'level': 'INFO',
            'propagate': False
        },
        'pynamodb': {
            'handlers': ['detailedStream'],
            'level': 'WARN',
            'propagate': False
        },
        'botocore': {
            'handlers': ['detailedStream'],
            'level': 'WARN',
            'propagate': False
        },
    }
}