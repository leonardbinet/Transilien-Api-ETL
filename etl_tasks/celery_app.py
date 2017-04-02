"""Celery app
"""

from os import sys, path

from celery import Celery
from celery.schedules import crontab

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from api_etl.utils_rdb import uri


app = Celery('etl_tasks',
             broker='amqp://guest:guest@localhost',
             backend='db+' + uri,  # 'amqp://guest:guest@localhost',
             include=['etl_tasks.tasks']
             )

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
)


app.conf.beat_schedule = {
    # Executes every two minutes
    'extract_api_two_minutes': {
        'task': 'tasks.extract_api_once_all_stations',
        'schedule': crontab(minute='*/2'),
    },
    # Executes every Monday morning at 7:30 a.m.
    'extract_schedule_weekly': {
        'task': 'tasks.extract_schedule',
        'schedule': crontab(hour=7, minute=30, day_of_week=1),
    },
}
