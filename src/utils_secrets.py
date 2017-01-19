"""
This module contains configuration information.
"""

import os
import json
import logging

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with open(os.path.join(BASE_DIR, 'secret.json')) as secrets_file:
    secrets = json.load(secrets_file)


def get_secret(setting, my_secrets=secrets):
    try:
        value = my_secrets[setting]
        # set as environment variable
        os.environ[setting] = value
        return my_secrets[setting]
    except KeyError:
        logging.warn("Impossible to get " + setting)

# Keep this information secret
API_USER = get_secret('API_USER')
API_PASSWORD = get_secret('API_PASSWORD')

MONGO_HOST = get_secret("MONGO_HOST")
MONGO_USER = get_secret("MONGO_USER")
MONGO_DB_NAME = get_secret("MONGO_DB_NAME")
MONGO_PASSWORD = get_secret("MONGO_PASSWORD")
