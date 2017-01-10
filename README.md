# API TRANSILIEN

## Setup

### Create virtual environment

```
conda create -n api_transilien python=3
# to activate it
source activate api_transilien
pip install -r requirements.txt
```

### Create secret.json file
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

## Extract data from Transilien API
To launch script: (default, cycle of 1200 seconds: 20 minutes)
```
python main.py extract
```
Or you can choose your cycle: for instance 2 minutes (120 seconds).
```
python main.py extract 120
```
