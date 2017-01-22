# https://www.internalpointers.com/post/run-painless-test-suites-python-unittest
from os import path, sys

if __name__ == '__main__':
    sys.path.append(path.dirname(path.abspath(__file__)))
    # Logging configuration
    from src.utils_misc import set_logging_conf
    set_logging_conf(log_name="test_runner.log", level="DEBUG")

import unittest

from tests import test_01_extract, test_02_schedules, test_03_match

# initialize the test suite
loader = unittest.TestLoader()
suite = unittest.TestSuite()

# add tests to the test suite
suite.addTests(loader.loadTestsFromModule(test_01_extract))
suite.addTests(loader.loadTestsFromModule(test_02_schedules))
suite.addTests(loader.loadTestsFromModule(test_03_match))

# initialize a runner, pass it your suite and run it
runner = unittest.TextTestRunner(verbosity=3)
result = runner.run(suite)
