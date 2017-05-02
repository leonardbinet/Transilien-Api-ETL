"""
Module used to interact with relational databases.
"""

import sqlalchemy
from sqlalchemy.orm import sessionmaker


from api_etl.utils_secrets import get_secret
from api_etl.models import RdbModel
from api_etl.utils_misc import build_uri


RDB_USER = get_secret("RDB_USER")
RDB_DB_NAME = get_secret("RDB_DB_NAME")
RDB_PASSWORD = get_secret("RDB_PASSWORD")
RDB_HOST = get_secret("RDB_HOST") or "localhost"
RDB_TYPE = get_secret("RDB_TYPE") or "postgresql"
RDB_PORT = get_secret("RDB_PORT") or 5432

uri = build_uri(
    user=RDB_USER,
    password=RDB_PASSWORD,
    database=RDB_DB_NAME,
    host=RDB_HOST,
    port=RDB_PORT,
    db_type=RDB_TYPE
)


class RdbProvider:
    """ `SQLAlchemy`_ support provider.

    This is built to connect to a single database. If multiple needed I would
    separate engines and sessions objects.

    You can:
    - get engine
    - get a session
    - create tables
    .. _SQLAlchemy: http://www.sqlalchemy.org/
    """

    def __init__(self, dsn=None):
        self._engine = sqlalchemy.create_engine(
            dsn or uri
        )
        # session maker is a class
        self._session_class = sessionmaker(bind=self._engine)

    def get_engine(self):
        """ Return an :class:`sqlalchemy.engine.Engine` object.
        :return: a ready to use :class:`sqlalchemy.engine.Engine` object.
        """
        return self._engine

    def get_session(self):
        """ Return an :class:`sqlalchemy.orm.session.Session` object.
        :return: a ready to use :class:`sqlalchemy.orm.session.Session` object.
        """
        return self._session_class()

    def create_tables(self):
        """ Creates table if not already present.
        """
        RdbModel.metadata.create_all(self._engine)
