#!/usr/bin/env python

import unittest
import os
import time
import configuration

TEMP_DB = '/tmp/test_marvin.db'
TEMP_LOG = '/tmp/test_marvin.log'


class TimingTestCase(unittest.TestCase):
    sch = None

    def __init__(self, *args, **kwargs):
        super(TimingTestCase, self).__init__(*args, **kwargs)

        if self.sch is None:
            configuration.select('marvinctld',
                                 preset={'inventory': {'sync': False},
                                         'database': TEMP_DB,
                                         'log': {'level': 50,
                                                 'file': TEMP_LOG}})

            from scheduler import Scheduler
            self.sch = Scheduler(refresh=True)
            self.assertIsNotNone(self.sch.db())

            self.assertTrue(os.path.isfile(TEMP_DB))

            userid, error = self.sch.create_user('admin', 'admin', 'admin')
            self.assertIsNotNone(userid)

            c = self.sch.db().cursor()
            c.execute("INSERT OR IGNORE INTO nodes VALUES (?, ?, ?, ?)",
                      ('1', 'test-node', 'active', 0))
            self.sch.db().commit()
            self.sch.set_node_types(1, 'test')

    def test_01_lots_of_recurring(self):
        userid = self.sch.get_users()[0]['id']
        now = time.time()
        until = self.sch.get_scheduling_period()[1]
        r = self.sch.allocate(userid, 'test', now + 500, 60, 1, 'test', '...',
                              {'recurrence': 'simple',
                               'period': 3600,
                               'until': until})
        self.assertEqual(r[2]['nodecount'], 1)
        self.assertEqual(r[2]['intervals'], 744)
        r = self.sch.allocate(userid, 'test', now + 750, 60, 1, 'test', '...',
                              {'recurrence': 'simple',
                               'period': 3600,
                               'until': until})
        self.assertEqual(r[2]['nodecount'], 1)
        self.assertEqual(r[2]['intervals'], 744)
        after = time.time()
        print "completed in %f seconds." % (after-now)
        self.assertLess(after-now, 3)  # completes in 3 seconds or less

    def test_02_find_slot(self):
        before = time.time()
        r = self.sch.find_slot(1)
        self.assertEqual(r[0][0]['nodecount'], 1)
        r = self.sch.find_slot(1, nodetypes='fail')
        self.assertIsNone(r[0])
        r = self.sch.find_slot(1, duration=500, nodes=['1'],
                               nodetypes='test,-foo')
        self.assertIsNotNone(r[0])
        self.assertEqual(r[0][0]['nodecount'], 1)
        r = self.sch.find_slot(1, duration=500, nodes=['500', '600'])
        self.assertIsNone(r[0])
        after = time.time()
        print "completed in %f seconds." % (after-before)
        self.assertLess(after-before, 3)  # completes in 3 seconds or less

    def test_99_cleanup(self):
        os.unlink(TEMP_DB)
