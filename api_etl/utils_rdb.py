from os import sys, path
import logging
import sqlite3
import psycopg2
import sqlalchemy
# import asyncio
# import aiopg


if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from api_etl.utils_misc import set_logging_conf
    set_logging_conf(log_name="sqlite_direct.log")

from api_etl.utils_secrets import get_secret
from api_etl.settings import sqlite_path

logger = logging.getLogger(__name__)

POSTGRES_USER = get_secret("POSTGRES_USER")
POSTGRES_DB_NAME = get_secret("POSTGRES_DB_NAME")
POSTGRES_PASSWORD = get_secret("POSTGRES_PASSWORD")
POSTGRES_HOST = get_secret("POSTGRES_HOST") or "localhost"
POSTGRES_PORT = get_secret("POSTGRES_PORT") or 5432


def rdb_connection(db="postgres"):
    uri = build_uri()
    if db == "sqlite":
        return sqlite3.connect(sqlite_path)
    elif db == "postgres":
        conn = psycopg2.connect(uri)
        conn.autocommit = True
        return conn
    elif db == "postgres_alch":
        # The return value of create_engine() is our connection object
        conn = sqlalchemy.create_engine(uri, client_encoding='utf8')
        # We then bind the connection to MetaData()
        # meta = sqlalchemy.MetaData(bind=con, reflect=True)
        return conn
    else:
        raise ValueError(
            "db should be one of these: 'postgres', 'postgres_alch', 'sqlite'")


def build_uri(user=POSTGRES_USER, password=POSTGRES_PASSWORD, db=POSTGRES_DB_NAME, host=POSTGRES_HOST, port=POSTGRES_PORT):
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    uri = 'postgresql://{}:{}@{}:{}/{}'
    uri = uri.format(user, password, host, port, db)
    return uri


"""
# Not working postgres async for now

def postgres_async_query_get_trip_ids(items, yyyymmdd_day):
    dsn = 'dbname=%s user=%s password=%s' % (
        POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD)

    async def fetch(item, cursor):
        try:
            train_num = item["num"]
            query = "SELECT trip_id FROM trips_ext WHERE train_num='%s' AND start_date<='%s' AND end_date>='%s';" % (
                train_num, yyyymmdd_day, yyyymmdd_day)

            await cursor.execute(query)
            trip_ids = await cursor.fetchone()

            # Check number of results
            if not trip_ids:
                logger.warning("No matching trip_id")
                return "nothing"
            elif len(trip_ids) == 1:
                trip_id = trip_ids[0][0]
                logger.debug("Found trip_id: %s" % trip_ids)
                item["trip_id"] = trip_id
                return item
            else:
                logger.warning("Multiple trip_ids found: %d matches: %s" %
                               (len(trip_ids), trip_ids))
                return "lots"
        except:
            logger.debug(
                "Error getting item %s trip_id" % train_num)

    async def run(items):
        tasks = []
        async with aiopg.create_pool(dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for item in items:
                        task = asyncio.ensure_future(
                            fetch(item, cur))
                        tasks.append(task)

                    responses = await asyncio.gather(*tasks)
                    # you now have all response bodies in this variable
                    # print(responses)
                    return responses

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(items))
    loop.run_until_complete(future)
    return future.result()
"""
