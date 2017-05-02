"""Celery app
"""

from os import sys, path
from datetime import timedelta

from celery import Celery
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from celery import group

# import logging
# import logging.config
# logging.config.fileConfig('logging.conf')
# logger = logging.getLogger('root')

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from api_etl.utils_rdb import uri
from api_etl.extract_api import operate_one_cycle
from api_etl.extract_schedule import ScheduleExtractorRDB
from api_etl.query import DBQuerier
from api_etl.utils_misc import StationProvider, get_paris_local_datetime_now

logger = get_task_logger(__name__)

app = Celery('etl_tasks.celery_app',
             broker='amqp://guest:guest@localhost',
             backend='db+' + uri,  # 'amqp://guest:guest@localhost',
             )

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
)

app.conf.beat_schedule = {
    # Executes every two minutes
    'extract_api_two_minutes': {
        'task': 'etl_tasks.celery_app.extract_api_once_all_stations',
        'schedule': crontab(minute='*/2'),
    },
    # Executes every Monday morning at 7:30 a.m.
    'extract_schedule_weekly': {
        'task': 'etl_tasks.celery_app.extract_schedule',
        'schedule': crontab(hour=7, minute=30, day_of_week=1),
    },
    # Executes every Monday morning at 7:30 a.m.
    'save_flat_mongo_day': {
        'task': 'etl_tasks.celery_app.mongo_save_all_stop_times_from_yesterday',
        'schedule': crontab(hour=4, minute=30),
    },
}


@app.task
def extract_api_once_all_stations(station_filter=None):
    # Default: all stations, and max 300 queries per sec
    logger.info("Beginning single cycle extraction")
    operate_one_cycle(station_filter=station_filter)
    return True


@app.task
def extract_schedule():

    # This operation is done every week
    logger.info("Task: weekly update of gtfs files in two steps:"
                + "download, then save in database.")

    schex = ScheduleExtractorRDB()

    logger.info("Download files.")
    schex.download_gtfs_files()

    logger.info("Save in database.")
    schex.save_in_rdb()
    return True


@app.task
def mongo_save_flat_stop_times_for_station_day(station_id, yyyymmdd):
    # station_id is in 7 digits gtfs format
    logger.info("Station %s, on day %s." % (station_id, yyyymmdd))
    querier = DBQuerier()
    result = querier.station_trips_stops(
        station_id=station_id,
        yyyymmdd=yyyymmdd
    )
    result.batch_realtime_query(yyyymmdd=yyyymmdd)
    result.get_flat_dicts(realtime_only=False, normalize=True)
    logger.info("Save in Mongo")
    result.save_in_mongo()
    logger.info("Task finished")
    return len(result.get_flat_dicts(realtime_only=False, normalize=True))


@app.task
def mongo_save_all_stop_times_from_yesterday():
    yesterday_dt = get_paris_local_datetime_now() - timedelta(days=1)
    yyyymmdd = yesterday_dt.strftime("%Y%m%d")
    logger.info("Save all stations on day %s in Mongo." % yyyymmdd)

    sp = StationProvider()
    stations = sp.get_station_ids(gtfs_format=True)

    signatures = [mongo_save_flat_stop_times_for_station_day.s(
        station, yyyymmdd) for station in stations]

    t_group = group(signatures)
    t_group()
