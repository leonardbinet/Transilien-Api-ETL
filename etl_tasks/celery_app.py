"""Celery app
"""

from os import sys, path

from celery import Celery

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
