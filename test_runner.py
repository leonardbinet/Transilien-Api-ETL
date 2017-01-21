# https://www.internalpointers.com/post/run-painless-test-suites-python-unittest

import unittest

from tests import test_01_extract, test_02_schedules

# initialize the test suite
loader = unittest.TestLoader()
suite = unittest.TestSuite()

# add tests to the test suite
suite.addTests(loader.loadTestsFromModule(test_01_extract))
suite.addTests(loader.loadTestsFromModule(test_02_schedules))

# initialize a runner, pass it your suite and run it
runner = unittest.TextTestRunner(verbosity=3)
result = runner.run(suite)
