# https://www.internalpointers.com/post/run-painless-test-suites-python-unittest
from os import path, sys

if __name__ == '__main__':
    sys.path.append(path.dirname(path.abspath(__file__)))
    # Logging configuration
    from api_transilien_manager.utils_misc import set_logging_conf
    set_logging_conf(log_name="test_runner.log", level="DEBUG")

import unittest

from test import test_01_extract_api, test_01_extract_schedule, test_02_query_schedule, test_03_match_collections, test_utils_misc, test_utils_rdb, test_utils_mongo

# initialize the test suite
loader = unittest.TestLoader()
suite = unittest.TestSuite()

# add tests to the test suite
suite.addTests(loader.loadTestsFromModule(test_utils_rdb))
suite.addTests(loader.loadTestsFromModule(test_utils_mongo))
suite.addTests(loader.loadTestsFromModule(test_utils_misc))

suite.addTests(loader.loadTestsFromModule(test_01_extract_api))
suite.addTests(loader.loadTestsFromModule(test_01_extract_schedule))
suite.addTests(loader.loadTestsFromModule(test_02_query_schedule))
suite.addTests(loader.loadTestsFromModule(test_03_match_collections))

# initialize a runner, pass it your suite and run it
runner = unittest.TextTestRunner(verbosity=3)
result = runner.run(suite)
