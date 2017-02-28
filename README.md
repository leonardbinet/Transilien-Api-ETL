# API TRANSILIEN

## Setup

### EC2 Instance
You should set up an EC2 instance with ssh keys that you can access this way:
```
ssh -i "~/.ssh/aws-eb2" ubuntu@34.251.124.59
```

### Configuration
Create secret.json file: this is the file where the application will find credentials for the different apis.

You have to create a JSON file in the root directory (same level as main.py):
```
{
    "API_USER" : "your_api_user",
    "API_PASSWORD" : "your_api_password",

    "MONGO_HOST":"***",
    "MONGO_USER":"***",
    "MONGO_DB_NAME":"***",
    "MONGO_PASSWORD":"***",

    "AWS_ACCESS_KEY_ID":"***",
    "AWS_SECRET_ACCESS_KEY":"***",
    "AWS_DEFAULT_REGION":"eu-west-1"
}
```

Set configuration variables in fabfile or in conf file.

### Environment

All setup with fabfile:
Note: Fabric only works with python2, so you might have to launch it from a virtual environment.
```
# If your default python is 3
conda create --name fabric_env python=2
source activate fabric_env
pip install fabric

# If your default python is 2
pip install fabric

# Launch fabfile:
# For first time:
fab initial_deploy:host=ubuntu@ec2-54-154-184-96.eu-west-1.compute.amazonaws.com
# Then later, to update if needed (shorter operation):
fab deploy:host=ubuntu@ec2-54-154-184-96.eu-west-1.compute.amazonaws.com


# For test env
fab initial_deploy:host=ubuntu@ec2-54-229-174-254.eu-west-1.compute.amazonaws.com
fab deploy:host=ubuntu@ec2-54-229-174-254.eu-west-1.compute.amazonaws.com

```



## Extract data from Transilien API
After deploying this repository with the fabfile, the extraction will be automated (default: cycle of 120 seconds, for 3500 seconds).

To launch script manually: (default: cycle of 120 seconds, for 3500 seconds)
```
python main.py extract
```
Or you can choose your cycle: for instance 10 minutes (600 seconds).
```
python main.py extract 600
```

## Cron:


### Logs
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

### Check what is running (all processes):
https://doc.ubuntu-fr.org/faq_process
```
ps -ef
ps -p "5513" -o etime=
```
To kill: brutal(replace number by id from last command)
```
sudo pkill -9 5513
```
## MongoDB

real_departures Unique Compound Index: day/station/num:
(beware after midnight)
```

scheduled_departures.create_index( [("scheduled_departure_day", pymongo.ASCENDING), ("station_id", pymongo.ASCENDING), ("train_num",pymongo.ASCENDING)], unique=True)

real_departures.create_index( [("request_day", pymongo.ASCENDING), ("station", pymongo.ASCENDING), ("num",pymongo.DESCENDING)], unique=True)

real_departures.create_index("train_num")

real_departures.create_index("station_id")

real_departures.create_index("scheduled_departure_day")

# New version with new index
real_departures.create_index( [("expected_passage_day", pymongo.ASCENDING), ("station", pymongo.ASCENDING), ("train_num",pymongo.ASCENDING)], unique=True)

real_departures.create_index("train_num")

real_departures.create_index("station_id")

real_departures.create_index("scheduled_departure_day")

```

## Postgres:

All is done automatically by the fabric script, when deploying, assuming that you created the secret.json file.
```
sudo -u postgres psql

CREATE DATABASE api_transilien;
CREATE USER api_transilien_user WITH PASSWORD 'password';

ALTER ROLE api_transilien_user SET client_encoding TO 'utf8';
ALTER ROLE api_transilien_user SET default_transaction_isolation TO 'read committed';
ALTER ROLE api_transilien_user SET timezone TO 'UTC';

GRANT ALL PRIVILEGES ON DATABASE api_transilien TO api_transilien_user;
```

## Dynamo
All is handled automatically.

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
