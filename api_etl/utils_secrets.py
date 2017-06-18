""" This module contains a function to extract secrets from environment or a
secret file.
"""

from os import path, environ
import json
import logging

from api_etl.settings import __BASE_DIR__

logger = logging.getLogger(__name__)

try:
    with open(path.join(__BASE_DIR__, 'secret.json')) as secrets_file:
        secrets = json.load(secrets_file)
except FileNotFoundError:
    secrets = {}
    logger.info("No file")


def get_secret(setting, my_secrets=secrets, env=True):
    """
    Tries to find secrets either in secret file, or in environment variables.
    env > secret file
    Then, set it as environment variable and returns value.
    :param setting:
    :param my_secrets:
    :param env:
    """
    value = None
    # Try to get value from env then from file
    try:
        value = environ[setting]
        return value
    except KeyError:
        logger.debug("Impossible to get %s from environment" % setting)

    try:
        value = my_secrets[setting]
    except KeyError:
        logger.debug("Impossible to get %s from file" % setting)

    # If value found, set it back as env
    if value and env:
        environ[setting] = value
        return value
    else:
        logger.warning("%s not found." % setting)
