"""Celery app
"""

from os import sys, path

from celery import Celery
from celery.schedules import crontab
from celery.utils.log import get_task_logger

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from api_etl.utils_rdb import uri
from api_etl.extract_api import operate_one_cycle
from api_etl.extract_schedule import ScheduleExtractorRDB

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
