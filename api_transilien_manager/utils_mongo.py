from os import sys, path
from pymongo import MongoClient
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import logging
from urllib.parse import quote_plus
import datetime
import pytz

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from api_transilien_manager.utils_misc import set_logging_conf
    set_logging_conf(log_name="mongo_direct.log")

from api_transilien_manager.utils_secrets import get_secret

logger = logging.getLogger(__name__)

MONGO_HOST = get_secret("MONGO_HOST")
MONGO_USER = get_secret("MONGO_USER")
MONGO_DB_NAME = get_secret("MONGO_DB_NAME")
MONGO_PASSWORD = get_secret("MONGO_PASSWORD")


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
            logger.debug("Chunk inserted")
        except:
            logger.error("Could not save chunk")

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


def mongo_async_upsert_items(collection, item_list, index_fields):
    asy_collection = mongo_get_async_collection(collection)

    def mongo_get_replace_filter(item_to_upsert, index_fields):
        m_filter = {}
        for index_field in index_fields:
            m_filter[index_field] = item_to_upsert[index_field]
        return m_filter

    async def do_upsert(item_to_upsert, m_filter):
        try:
            result = await asy_collection.replace_one(m_filter, item_to_upsert, upsert=True)
            logger.debug("Item inserted")
            if not result.acknowledged:
                logger.error("Item %s not inserted" % item_to_upsert)
            # print("Result: %s" % result)

        except Exception as e:
            logger.error("Could not save item, error %s" % e)

    async def run(item_list):
        tasks = []
        for item_to_upsert in item_list:
            m_filter = mongo_get_replace_filter(item_to_upsert, index_fields)
            task = asyncio.ensure_future(
                do_upsert(item_to_upsert, m_filter))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        return responses

    # def print_responses(result):
    #    print(result)
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(item_list=item_list))
    loop.run_until_complete(future)
    return future.result()


def mongo_async_update_items(collection, item_query_update_list):
    asy_collection = mongo_get_async_collection(collection)

    async def do_update(item_query_update):
        try:
            find_query = item_query_update[0]
            update_query = item_query_update[1]
            result = await asy_collection.update_one(find_query, update_query)
            logger.debug("Item updated")
            if not result.acknowledged:
                logger.error("Item %s not updated" % item_query_update)
            # print("Result: %s" % result)

        except Exception as e:
            logger.error("Could not update item, error %s" % e)

    async def run(item_list):
        tasks = []
        for item_query_update in item_query_update_list:
            task = asyncio.ensure_future(do_update(item_query_update))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        return responses

    # def print_responses(result):
    #    print(result)
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(item_list=item_query_update_list))
    loop.run_until_complete(future)
    return future.result()


def mongo_move_day_data_to_other_col(yyyymmdd_day, old_col, new_col, day_field, del_original=False):
    logger.info("Moving data from %s to %s for day %s" %
                (old_col, new_col, yyyymmdd_day))

    # Lock for current day in Paris (can only move past days results)
    paris_tz = pytz.timezone('Europe/Paris')

    today_paris = paris_tz.localize(datetime.datetime.now())
    today_paris_str = today_paris.strftime("%Y%m%d")
    if yyyymmdd_day == today_paris_str:
        logger.warn(
            "Trying to move today's collected data: ABORT: it is forbidden to avoid data corruption (this data might be changed in realtime).")
        return False

    day_filter = {day_field: yyyymmdd_day}
    old_collection = mongo_get_collection(old_col)
    new_collection = mongo_get_collection(new_col)

    # Count number of elements that should be moved:
    old_col_initial_count = old_collection.find(day_filter).count()

    logger.info("There are %s items on day %s on %s." %
                (old_col_initial_count, yyyymmdd_day, old_col))

    new_col_initial_count = new_collection.find(day_filter).count()

    logger.info("There are %s items on day %s on %s." %
                (new_col_initial_count, yyyymmdd_day, new_col))

    if old_col_initial_count == 0:
        logger.warn(
            "There was no item matching this date query on %s colleciton: ABORT operation." % old_col)
        return False
    if new_col_initial_count > 0:
        logger.warn(
            "There are %s existing elements that might be deleted on %s colleciton: ABORT operation." % (new_col_initial_count, new_col))
        return False

    # Move data
    old_collection.aggregate(
        [{"$match": day_filter}, {"$out": new_col}])

    # Check if it has been moved
    new_col_final_count = new_collection.find(day_filter).count()
    logger.info("There are %s items on day %s on %s after operation." %
                (new_col_final_count, yyyymmdd_day, new_col))

    # Delete data in old_col
    if del_original:
        del_result = old_collection.delete_many(day_filter)
        logger.info("Deleted %s results from %s" %
                    (del_result.deleted_count, old_col))
