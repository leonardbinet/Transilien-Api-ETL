import unittest
import logging

from api_transilien_manager.utils_rdb import rdb_connection

logger = logging.getLogger(__name__)


class TestUtilsRdbFunctions(unittest.TestCase):

    def test_connection_rdb(self):
        """
        Check if we manage to get all rdb connections:
        - postgres (with psycopg2)
        - postgres (with alchemy)
        - sqlite
        """
        def check_connection(conn):
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM trips_ext LIMIT 1;")
                cursor.fetchone()
                logger.debug("Connection worked.")
                return True
            except Exception as e:
                logger.warn("Connection didn't work: %s" % e.with_traceback())
                return False

        def check_connection_alch(conn):
            try:
                conn.has_table("trips_ext")
                logger.debug("Connection worked.")
                return True
            except Exception as e:
                logger.warn("Connection didn't work: %s" % e.with_traceback())

        conn_psycopg2 = rdb_connection()
        self.assertTrue(check_connection(conn_psycopg2))

        conn_alchemy = rdb_connection("postgres_alch")
        self.assertTrue(check_connection_alch(conn_alchemy))

        conn_sqlite = rdb_connection("sqlite")
        self.assertTrue(check_connection(conn_sqlite))


if __name__ == '__main__':
    unittest.main()
