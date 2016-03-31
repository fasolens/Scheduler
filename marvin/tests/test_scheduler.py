#!/usr/bin/env python

import unittest
import os, time
import configuration
import simplejson as json

TEMP_DB = '/tmp/test_marvin.db'
TEMP_LOG = '/tmp/test_marvin.log'


class SchedulerTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(SchedulerTestCase, self).__init__(*args, **kwargs)

        configuration.select('marvinctld',
                             preset={'inventory': {'sync': False},
                                     'database': TEMP_DB,
                                     'log': {'level': 50,
                                             'file': TEMP_LOG}})

        from scheduler import Scheduler
        self.sch = Scheduler()

    def test_00_db_exists(self):
        self.assertTrue(os.path.isfile(TEMP_DB))
        self.assertIsNotNone(self.sch.db())

    def test_01_create_user(self):
        userid, error = self.sch.create_user('admin', 'admin', 'admin')
        self.assertIsNotNone(userid)
        user = self.sch.get_users(ssl='admin')
        self.assertIsNotNone(user)
        self.assertEqual(user[0].get('id', None), 1)
        self.assertGreater(user[0].get('quota_time', 0), 0)
        self.assertGreater(user[0].get('quota_data', 0), 0)
        self.assertGreater(user[0].get('quota_storage', 0), 0)
        role = self.sch.get_role(ssl='admin')
        self.assertEqual(role, 'admin')
        # not an user
        user = self.sch.get_users(userid=5)
        self.assertIsNone(user)

    def test_02_create_node(self):
        c = self.sch.db().cursor()
        c.execute("INSERT OR IGNORE INTO nodes VALUES (?, ?, ?, ?)",
                  ('1', 'test-node', 'active', 0))
        self.sch.db().commit()
        self.sch.set_node_types(1, 'test')
        result = self.sch.get_nodes(nodetype='test')
        self.assertEqual(result[0].get('id', None), 1)

    def test_03_delete_user(self):
        userid, error = self.sch.create_user('foo', 'foo', 'foo')
        self.assertIsNotNone(userid)
        ok = self.sch.delete_user(userid)
        self.assertTrue(ok)

    def test_04_get_empty_schedule(self):
        self.assertEqual(self.sch.get_schedule(), [])

    def test_05_get_empty_experiments(self):
        self.assertIsNone(self.sch.get_experiments())

    def test_06_get_scheduling_period(self):
        p = self.sch.get_scheduling_period()
        self.assertGreater(p[1]-p[0], 0)

    def test_10_allocate(self):
        now = int(time.time())
        # basic a few minutes into the future
        r = self.sch.allocate(1,'test', now + 500, 500, 1, 'test', '...', {})
        self.assertEqual(r[2]['nodecount'], 1)
        # not available
        r = self.sch.allocate(1,'test', now + 500, 500, 1, 'test', '...', {})
        self.assertEqual(r[2]['available'], 0)
        # too soon
        r = self.sch.allocate(1,'test', now, 500, 1, 'test', '...', {})
        self.assertIsNone(r[0])
        # too short
        r = self.sch.allocate(1,'test', now + 1500, 1, 1, 'test', '...', {})
        self.assertIsNone(r[0])
        # zero node count
        r = self.sch.allocate(1,'test', now + 1500, 500, 0, 'test', '...', {})
        self.assertIsNone(r[0])
        # too many nodes
        r = self.sch.allocate(1,'test', now + 1500, 500, 2, 'test', '...', {})
        self.assertEqual(r[2]['available'], 1)
        # too much storage requested
        r = self.sch.allocate(1,'test', now + 1500, 500, 1, 'test', '...',
                              {'storage': 501 * 1000000})
        self.assertEqual(r[2]['requested'], 501 * 1000000)

    def test_11_recurrence(self):
        now = int(time.time())
        r = self.sch.allocate(1,'test', now + 1500, 500, 1, 'test', '...',
                {'recurrence':'simple',
                 'period': 3600,
                 'until': now + 1500 + 3600 * 2 + 500
                })
        self.assertEqual(r[2]['nodecount'], 1)
        self.assertEqual(r[2]['intervals'], 3)
        # check that the tasks are different, and of equal length
        t = self.sch.get_schedule(expid = 2)
        self.assertGreater(t[1]['start'], t[0]['start'])
        self.assertGreater(t[1]['stop'], t[0]['stop'])
        self.assertGreater(t[0]['stop'], t[0]['start'])
        self.assertGreater(t[1]['stop'], t[1]['start'])
        self.assertEqual(t[0]['stop'] - t[0]['start'],
                         t[1]['stop'] - t[1]['start'],)
        # truncating until
        r = self.sch.allocate(1,'test', now + 1500, 500, 1, 'test', '...',
                {'recurrence':'simple',
                 'period': 3600,
                 'until': now + 100000000000
                 })
        self.assertLess(r[2]['stop'], now + 100000000000)

    def test_12_find_slot(self):
        r = self.sch.find_slot(1)
        self.assertEqual(r[0][0]['nodecount'], 1)
        r = self.sch.find_slot(1, nodetypes='fail')
        self.assertIsNone(r[0])
        r = self.sch.find_slot(1, duration=500, nodes=['1'])
        self.assertIsNotNone(r[0])
        self.assertEqual(r[0][0]['nodecount'], 1)
        r = self.sch.find_slot(1, duration=500, nodes=['500','600'])
        self.assertIsNone(r[0])


    def test_21_delete_experiment(self):
        r = self.sch.delete_experiment(3)
        self.assertEqual(r[0],0)
        r = self.sch.delete_experiment(1)
        self.assertEqual(r[0],1)


    def test_99_cleanup(self):
        os.unlink(TEMP_DB)
        os.unlink(TEMP_LOG)
