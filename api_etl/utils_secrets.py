"""
This module contains configuration information.
"""

import os
import json
import logging
from api_etl.settings import BASE_DIR

logger = logging.getLogger(__name__)


with open(os.path.join(BASE_DIR, 'secret.json')) as secrets_file:
    secrets = json.load(secrets_file)


def get_secret(setting, my_secrets=secrets, env=False):
    try:
        value = my_secrets[setting]
        if env:
            os.environ[setting] = value
        return value
    except KeyError:
        logger.warn("Impossible to get " + setting)
