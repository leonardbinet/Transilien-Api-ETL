"""Celery app
"""

from os import sys, path
import logging
import logging.config

from celery import Celery
from celery.schedules import crontab

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from api_etl.settings import __LOGGING_CONFIG__, __ACCEPTED_LINES__
from api_etl.utils_rdb import uri
from api_etl.extract_api import operate_one_cycle
from api_etl.extract_schedule import ScheduleExtractorRDB
from api_etl.regressor_train import RegressorTrainer
from api_etl.builder_feature_matrix import TrainingSetBuilder
from api_etl.utils_misc import get_paris_local_datetime_now

logging.config.dictConfig(__LOGGING_CONFIG__)
logger = logging.getLogger(__name__)

app = Celery('etl_tasks.celery_app',
             broker='amqp://guest:guest@localhost',
             backend='db+' + uri,  # 'amqp://guest:guest@localhost',
             )

# Optional configuration
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

    logger.info("Save in S3.")
    schex.save_gtfs_in_s3()

    logger.info("Save in database.")
    schex.save_in_rdb()
    return True

@app.task
def train_models(lines=None):
    if isinstance(lines, list):
        for line in lines:
            assert line in __ACCEPTED_LINES__

    if lines is None:
        lines = __ACCEPTED_LINES__

    logger.info("Beginning models training for lines %s." % lines)

    for line in lines:
        logger.info("Line %s." % line)
        rt = RegressorTrainer(line=line, auto=True)
        logger.info(rt.score_pipeline())
        rt.save_in_database()

    return True

@app.task
def build_training_sets_last_day():

    logger.info("Beginning building training set for yesterday.")
    day = get_paris_local_datetime_now().strftime("%Y%m%d")
    tsb = TrainingSetBuilder(start=day, end=day, tempo=30)
    tsb.create_training_sets()

    return True
