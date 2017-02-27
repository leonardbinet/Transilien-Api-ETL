import unittest
import logging

from api_etl.utils_mongo import mongo_get_collection

logger = logging.getLogger(__name__)


class TestUtilsMongoFunctions(unittest.TestCase):

    def test_mongo_get_collection(self):
        """
        Check if we manage to get a connection and execute a query.
        """
        def check_connection():
            try:
                collection = mongo_get_collection("real_departures_2")
                collection.find_one()
                logger.debug("Mongo connection worked.")
                return True
            except Exception as e:
                logger.error("Connection didn't work: %s" % e.with_traceback())
                return False

        mongo_status = check_connection()
        self.assertTrue(mongo_status)

if __name__ == '__main__':
    unittest.main()
