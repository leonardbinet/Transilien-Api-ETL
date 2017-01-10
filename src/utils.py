from pymongo import MongoClient

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
