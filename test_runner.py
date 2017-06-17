"""
Module made to launch all tests.
"""
# https://www.internalpointers.com/post/run-painless-test-suites-python-unittest

from os import path, sys
import unittest

if __name__ == '__main__':
    sys.path.append(path.dirname(path.abspath(__file__)))
    from api_etl.utils_misc import set_logging_conf
    set_logging_conf(log_name="tests.log", level="DEBUG")


from tests import (
    test_extract_api, test_extract_schedule, test_query_schedule,
    test_match_ids, test_utils_misc, test_utils_rdb
)

# initialize the test suite
loader = unittest.TestLoader()
suite = unittest.TestSuite()

# add tests to the test suite
suite.addTests(loader.loadTestsFromModule(test_utils_rdb))
suite.addTests(loader.loadTestsFromModule(test_utils_mongo))
suite.addTests(loader.loadTestsFromModule(test_utils_misc))

suite.addTests(loader.loadTestsFromModule(test_extract_api))
suite.addTests(loader.loadTestsFromModule(test_extract_schedule))
suite.addTests(loader.loadTestsFromModule(test_query_schedule))
suite.addTests(loader.loadTestsFromModule(test_match_ids))

# initialize a runner, pass it your suite and run it
runner = unittest.TextTestRunner(verbosity=3)
result = runner.run(suite)
