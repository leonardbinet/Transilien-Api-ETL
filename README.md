# API TRANSILIEN

## Overview

This repository is part of a broader project with SNCF’s R&D department to provide arrival time predictions for trains in Paris area.

A repository details how this project is deployed through Vagrant and Salt automation tools.

Another details details how the website and the API built for this project work.


#### Tasks overview
![tasks](ressources/ETL_SNCF_tasks.png)

#### Diagram
![diagram](ressources/ETL_SNCF_diagram.png)

## What it does

#### Task 1: scheduled departure times (from SNCF website in GTFS format):

- download schedules from SNCF website (csv format)
- save it in relational database (optionally)
- find each day which trips should pass, and at what time trains should pass in stations
- save it in Dynamo ‘scheduled_departures’ table

#### Task 2: real departure times (from Transilien’s API)

- extract data from transilien’s api
- transform XML received by api into json serializable objects
- query Dynamo ‘scheduled_departures’ table (can also query Postgres, but is much slower) to find out for each train predicted to arrive in a station, which trip_id is linked, and at what time it was scheduled to arrive.
- enrich these objects
- save it in Dynamo ‘real_departures’ table



## Documentation

You will find detailed documentation here.
[HERE](https://leonardbinet.github.io/)

## SSH reminder
```
ssh -i "~/.ssh/aws-eb2" ubuntu@34.251.124.59
```

### Configuration

Either use [salt-vagrant repository](https://github.com/leonardbinet/Salt-Vagrant-master-mode) explaining how to set this project up.

Or create secret.json file: this is the file where the application will find credentials for the different apis.

You have to create a JSON file in the root directory (same level as main.py):
```
{
    "API_USER" : "your_api_user",
    "API_PASSWORD" : "your_api_password",

    "AWS_ACCESS_KEY_ID":"***",
    "AWS_SECRET_ACCESS_KEY":"***",
    "AWS_DEFAULT_REGION":"eu-west-1"
}
```


## Debug

```
cat /var/log/syslog
```
Or better:
```
grep CRON /var/log/syslog
```
Check what will be run:
```
run-parts --test /etc/cron.hourly
```

```
ps -ef
ps -p "5513" -o etime=
```
To kill: brutal(replace number by id from last command)
```
sudo pkill -9 5513
```


## Documentation
To generate documentation:

First activate your virtualenv (you should have Sphinx and sphinx_rtd_theme installed), from root directory:
```
# create structure
sphinx-apidoc --separate -f -o docs api_etl

# generate html
cd docs
make html
```
You will find documentation in `docs/_build` directory

*Warning*: beware of secrets. If your secrets are set in the secret.json file, they might be exposed through `get_secret` function parameters in documentation.
