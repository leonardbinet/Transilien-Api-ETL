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
fab full_deploy:host=ubuntu@ec2-54-229-162-229.eu-west-1.compute.amazonaws.com
# Then later, to update if needed (shorter operation):
fab deploy:host=ubuntu@ec2-54-229-162-229.eu-west-1.compute.amazonaws.com

```



## Extract data from Transilien API
To launch script: (default, cycle of 1200 seconds: 20 minutes)
```
python main.py extract
```
Or you can choose your cycle: for instance 2 minutes (120 seconds).
```
python main.py extract 120
```
