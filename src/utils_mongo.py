from os import sys, path
from pymongo import MongoClient
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import ipdb

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
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


def build_mongo_uri(host=MONGO_HOST, user=MONGO_USER, password=MONGO_PASSWORD, port=None, database=None):
    uri = "mongodb://"
    if user and password:
        uri += "%s:%s@" % (quote_plus(user), quote_plus(password))
    uri += host
    if port:
        uri += ":" + str(port)
    if database:
        uri += "/%s" % quote_plus(database)
    return uri


def get_mongoclient(max_delay=15000):
    uri = build_mongo_uri()
    client = MongoClient(uri, serverSelectionTimeoutMS=max_delay)
    return client


def get_async_mongoclient():
    uri = build_mongo_uri()
    client = AsyncIOMotorClient(uri)
    return client


def mongo_get_async_collection(collection):
    c = get_async_mongoclient()
    db = c[MONGO_DB_NAME]
    collection = db[collection]
    return collection


def mongo_get_collection(collection):
    c = get_mongoclient()
    db = c[MONGO_DB_NAME]
    collection = db[collection]
    return collection


def mongo_async_save_chunks(collection, chunks_list):
    asy_collection = mongo_get_async_collection(collection)

    async def do_insert_many(chunk):
        try:
            result = await asy_collection.insert_many(chunk)
            # print(result.inserted_ids)
        except:
            print("Could not save chunk")

    async def run(chunks_list):
        tasks = []
        # Fetch all responses within one Client session,
        # keep connection alive for all requests.

        for i, chunk in enumerate(chunks_list):
            task = asyncio.ensure_future(
                do_insert_many(chunk))
            tasks.append(task)
            # print("Chunk %d" % i)

        # all response in this variable
        responses = await asyncio.gather(*tasks)
        return responses

    # def print_responses(result):
    #    print(result)
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(chunks_list))
    loop.run_until_complete(future)
    return future.result()
