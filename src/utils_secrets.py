"""
This module contains configuration information.
"""

import os
import json
import logging
from src.settings import BASE_DIR

logger = logging.getLogger(__name__)


with open(os.path.join(BASE_DIR, 'secret.json')) as secrets_file:
    secrets = json.load(secrets_file)


def get_secret(setting, my_secrets=secrets):
    try:
        value = my_secrets[setting]
        # set as environment variable
        # os.environ[setting] = value
        return my_secrets[setting]
    except KeyError:
        logger.warn("Impossible to get " + setting)
