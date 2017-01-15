from pymongo import MongoClient

from src.utils_secrets import get_secret


MONGO_HOST = get_secret("MONGO_HOST")
MONGO_USER = get_secret("MONGO_USER")
MONGO_DB_NAME = get_secret("MONGO_DB_NAME")
MONGO_PASSWORD = get_secret("MONGO_PASSWORD")

try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus


def connect_mongoclient(host, user=None, password=None, port=None, database=None, max_delay=15000):
    # Build URI
    uri = "mongodb://"
    if user and password:
        uri += "%s:%s@" % (quote_plus(user), quote_plus(password))
    uri += host
    if port:
        uri += ":" + str(port)
    if database:
        uri += "/%s" % quote_plus(database)
    client = MongoClient(uri, serverSelectionTimeoutMS=max_delay)
    return client


def mongo_get_collection(collection):
    c = connect_mongoclient(
        host=MONGO_HOST, user=MONGO_USER, password=MONGO_PASSWORD)
    db = c[MONGO_DB_NAME]
    collection = db[collection]
    return collection
