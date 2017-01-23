from os import sys, path
import asyncio
import logging
from urllib.parse import quote_plus
import datetime
import pytz
import sqlite3


if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from src.utils_misc import set_logging_conf
    set_logging_conf(log_name="sqlite_direct.log")

from src.settings import sqlite_path

logger = logging.getLogger(__name__)


def sqlite_get_connection():
    return = sqlite3.connect(sqlite_path)


def sqlite_get_cursor():
    return = sqlite3.connect(sqlite_path).cursor()


def sqlite_save_df_in_table(df, table_name, index_label, if_exists='append'):
    con = sqlite_get_connection()
    df.to_sql(name_name, con, flavor=None, schema=None, if_exists=if_exists,
              index=False, index_label=index_label, chunksize=None, dtype=None)
