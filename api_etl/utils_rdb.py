"""
Module used to interact with Dynamo databases.
"""

import logging
import collections
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from api_etl.utils_secrets import get_secret
from api_etl.models import Model

logger = logging.getLogger(__name__)

RDB_USER = get_secret("RDB_USER")
RDB_DB_NAME = get_secret("RDB_DB_NAME")
RDB_PASSWORD = get_secret("RDB_PASSWORD")
RDB_HOST = get_secret("RDB_HOST") or "localhost"
RDB_TYPE = get_secret("RDB_TYPE") or "postgresql"
RDB_PORT = get_secret("RDB_PORT") or 5432


def build_dsn(user=RDB_USER, password=RDB_PASSWORD, db=RDB_DB_NAME, host=RDB_HOST, port=RDB_PORT, db_type=RDB_TYPE):
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    if user and host:
        dsn = '{}://{}:{}@{}:{}/{}'
        dsn = dsn.format(db_type, user, password, host, port, db)
    else:
        dsn = 'sqlite:///application.db'
    return dsn


class Provider:
    """ `SQLAlchemy`_ support provider.

    This is built to connect to a single database. If multiple needed I would separate engines and sessions objects.

    You can:
    - get engine
    - get a session
    - create tables
    .. _SQLAlchemy: http://www.sqlalchemy.org/
    """

    def __init__(self, dsn=None):
        self._engine = sqlalchemy.create_engine(dsn or build_dsn())
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
        Model.metadata.create_all(self._engine)


class ResultSerializer():

    def __init__(self, result):
        if isinstance(result, list):
            self.results = result
        else:
            self.results = [result]

        self.nested_dict = []
        self._to_nested_dict()

        self.flat_dict = []
        self._to_flat_dict()

    def _to_nested_dict(self):
        for el in self.results:
            mydict = el._asdict()
            for key, value in mydict.items():
                try:
                    mydict[key] = value.__dict__
                    del mydict[key]["_sa_instance_state"]
                except:
                    pass
            self.nested_dict.append(mydict)

    def _to_flat_dict(self):
        for el in self.nested_dict:
            assert isinstance(el, dict)
            new_el = self._flatten(el)
            self.flat_dict.append(new_el)

    def _flatten(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(self._flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
