#!/usr/bin/env python

import unittest
from test_scheduler import *

suite = unittest.TestLoader().loadTestsFromTestCase(SchedulerTestCase)
unittest.TextTestRunner(verbosity=2).run(suite)
