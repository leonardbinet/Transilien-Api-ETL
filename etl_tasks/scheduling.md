# Celery:
http://docs.celeryproject.org/en/latest/getting-started/first-steps-with-celery.html#id4

Requirements:

- a broker to send messages: RabbitMQ, server must be running:
command:
`rabbitmq-server`
- Celery installed through pip, app launched:
`celery -A etl_tasks.celery_app worker -l info`
celery -A proj worker -l info

celery -A etl_tasks.celery_app beat

## Components:

### Celery app:

Entry-point for everything you want to do in Celery:
- creating tasks
- managing workers,
It must be possible for other modules to import it.

```
# tasks.py
from celery import Celery

app = Celery('tasks', broker='pyamqp://guest@localhost//')

@app.task
def add(x, y):
    return x + y
```
### Worker

We need then a worker to launch the app.
`celery -A tasks worker --loglevel=info`

Kill workers:
`pkill -f "celery worker"``

### Set backend for traceability
Database

## Call and schedule tasks: Beat

We can ask the worker to run tasks directly, or we can schedule it.
