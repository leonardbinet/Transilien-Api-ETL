from os import sys, path
import asyncio
import logging
from urllib.parse import quote_plus
import datetime
import pytz
import sqlite3
import psycopg2
import sqlalchemy
import asyncio
import aiopg


if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from api_transilien_manager.utils_misc import set_logging_conf
    set_logging_conf(log_name="sqlite_direct.log")

from api_transilien_manager.utils_secrets import get_secret
from api_transilien_manager.settings import sqlite_path

logger = logging.getLogger(__name__)

POSTGRES_USER = get_secret("POSTGRES_USER")
POSTGRES_DB_NAME = get_secret("POSTGRES_DB_NAME")
POSTGRES_PASSWORD = get_secret("POSTGRES_PASSWORD")
POSTGRES_HOST = get_secret("POSTGRES_HOST")


def rdb_connection(db="postgres"):
    if db == "postgres":
        return postgres_get_connection()
    elif db == "sqlite":
        return sqlite_get_connection()
    elif db == "postgres_alch":
        con, meta = postgres_get_connection_alch()
        return con
    else:
        raise ValueError(
            "db should be one of these: 'postgres', 'postgres_alch', 'sqlite'")


def postgres_get_connection_alch(user=POSTGRES_USER, password=POSTGRES_PASSWORD, db=POSTGRES_DB_NAME, host='localhost', port=5432):
    '''Returns a connection and a metadata object'''
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)
    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, client_encoding='utf8')
    # We then bind the connection to MetaData()
    meta = sqlalchemy.MetaData(bind=con, reflect=True)
    return con, meta


def postgres_get_connection(user=POSTGRES_USER, password=POSTGRES_PASSWORD, db=POSTGRES_DB_NAME, host='localhost', port=5432):
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)
    conn = psycopg2.connect(url)
    conn.autocommit = True
    return conn


def postgres_save_df_in_table(df, table_name, index=False, index_label=None, if_exists='append'):
    con, meta = postgres_get_connection_alch()
    df.to_sql(table_name, con, schema=None, if_exists=if_exists,
              index=index, index_label=index_label, chunksize=None, dtype=None)


def sqlite_get_connection():
    return sqlite3.connect(sqlite_path)


def sqlite_get_cursor():
    return sqlite3.connect(sqlite_path).cursor()


def sqlite_save_df_in_table(df, table_name, index=False, index_label=None, if_exists='append'):
    con = sqlite_get_connection()
    df.to_sql(table_name, con, schema=None, if_exists=if_exists,
              index=index, index_label=index_label, chunksize=None, dtype=None)


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
