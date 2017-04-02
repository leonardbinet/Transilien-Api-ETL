"""Celery's tasks
"""
import sys
from os import sys, path
from celery.utils.log import get_task_logger


# if not called from right location
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from etl_tasks.celery_app import app
from api_etl.extract_api import operate_one_cycle
from api_etl.extract_schedule import ScheduleExtractorRDB

logger = get_task_logger(__name__)


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
