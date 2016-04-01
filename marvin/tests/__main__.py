#!/usr/bin/env python

import unittest
from test_scheduler import *
from test_timing import *

for test in [SchedulerTestCase, TimingTestCase]:
    suite = unittest.TestLoader().loadTestsFromTestCase(test)
    unittest.TextTestRunner(verbosity=2).run(suite)
