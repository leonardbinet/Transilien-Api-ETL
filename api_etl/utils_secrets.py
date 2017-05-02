""" This module contains a function to extract secrets from environment or a
secret file.
"""

import os
from os import path
import json
import logging
from api_etl.settings import BASE_DIR

try:
    with open(path.join(BASE_DIR, 'secret.json')) as secrets_file:
        secrets = json.load(secrets_file)
except FileNotFoundError:
    secrets = {}
    logging.info("No file")


def get_secret(setting, my_secrets=secrets, env=True):
    """
    Tries to find secrets either in secret file, or in environment variables.
    env > secret file
    Then, set it as environment variable and returns value.
    """
    value = None
    # Try to get value from env then from file
    try:
        value = os.environ[setting]
        return value
    except KeyError:
        logging.debug("Impossible to get %s from environment" % setting)

    try:
        value = my_secrets[setting]
    except KeyError:
        logging.debug("Impossible to get %s from file" % setting)

    # If value found, set it back as env
    if value and env:
        os.environ[setting] = value
        return value
    else:
        logging.warning("%s not found." % setting)
