from os import sys, path
import asyncio
import logging
from urllib.parse import quote_plus
import datetime
import pytz
import sqlite3
import psycopg2
import sqlalchemy

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from src.utils_misc import set_logging_conf
    set_logging_conf(log_name="sqlite_direct.log")

from src.utils_secrets import get_secret
from src.settings import sqlite_path

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


def postgres_get_connection():
    conn = psycopg2.connect(dbname=POSTGRES_DB_NAME,
                            user=POSTGRES_USER, password=POSTGRES_PASSWORD)
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
