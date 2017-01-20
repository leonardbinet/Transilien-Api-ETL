# API TRANSILIEN

## Setup

### EC2 Instance
You should set up an EC2 instance with ssh keys that you can access this way:
```
ssh -i "~/.ssh/aws-eb2" ubuntu@ec2-54-229-162-229.eu-west-1.compute.amazonaws.com
```

### Configuration
Create secret.json file: blablabla

It is needed to create a JSON file in the root directory (same level as main.py):
```
{
    "API_USER" : "your_api_user",
    "API_PASSWORD" : "your_api_password",

    "MONGO_HOST":"***",
    "MONGO_USER":"***",
    "MONGO_DB_NAME":"***",
    "MONGO_PASSWORD":"***"
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
fab initial_deploy:host=ubuntu@ec2-54-229-162-229.eu-west-1.compute.amazonaws.com
# Then later, to update if needed (shorter operation):
fab deploy:host=ubuntu@ec2-54-229-162-229.eu-west-1.compute.amazonaws.com

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
ps -e
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
real_departures.create_index( [("request_day", pymongo.DESCENDING), ("station", pymongo.ASCENDING), ("num",pymongo.DESCENDING)], unique=True)

scheduled_departures.create_index( [("scheduled_departure_day", pymongo.DESCENDING), ("station_id", pymongo.ASCENDING), ("train_num",pymongo.DESCENDING)], unique=True)

real_departures.create_index("train_num")

real_departures.create_index("station_id")

real_departures.create_index("scheduled_departure_day")

```
