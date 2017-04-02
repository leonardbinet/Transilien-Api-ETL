# Celery:
http://docs.celeryproject.org/en/latest/getting-started/first-steps-with-celery.html#id4

Requirements:

- a broker to send messages: RabbitMQ
`sudo apt-get install rabbitmq-server`

- Celery installed:
`pip install celery`


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


### Set backend for traceability
Database

## Call and schedule tasks: Beat

We can ask the worker to run tasks directly, or we can schedule it.
