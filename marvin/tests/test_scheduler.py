#!/usr/bin/env python

import unittest
import os, time
import configuration
import simplejson as json

TEMP_DB = '/tmp/test_marvin.db'
TEMP_LOG = '/tmp/test_marvin.log'


class SchedulerTestCase(unittest.TestCase):
    sch = None

    def __init__(self, *args, **kwargs):
        super(SchedulerTestCase, self).__init__(*args, **kwargs)

        if self.sch is None:
            configuration.select('marvinctld',
                                 preset={'inventory': {'sync': False},
                                         'database': TEMP_DB,
                                         'repository': {
                                                 'deployment':'server.xyz'},
                                         'log': {'level': 50,
                                                 'file': TEMP_LOG}})

            from scheduler import Scheduler
            self.sch = Scheduler()

            c = self.sch.db().cursor()
            now = int(time.time())
            c.execute("INSERT OR IGNORE INTO nodes VALUES (?, ?, ?, ?)",
                  ('1', 'test-node', 'active', now))
            c.execute("INSERT OR IGNORE INTO nodes VALUES (?, ?, ?, ?)",
                  ('2', 'test-node 2', 'active', now))
            c.execute("INSERT OR REPLACE INTO node_interface "
                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  ('1', 'a', 'ab', 'abc', 'abcd', 0, 0, 0, 0, 0, 0, now))
            c.execute("INSERT OR REPLACE INTO node_interface "
                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  ('2', 'a', 'ab', 'abc', 'abcd', 0, 0, 0, 0, 0, 0, now))
            self.sch.db().commit()
            self.sch.set_node_types(1, 'status:test')

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

    def test_02_created_node(self):
        result = self.sch.get_nodes(nodetype='status:test')
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
        r = self.sch.allocate(1,'test', now + 500, 500, 1, 'status:test,-status:foo,-status:bar', '...', {})
        self.assertEqual(r[2]['nodecount'], 1)
        # not available
        r = self.sch.allocate(1,'test', now + 500, 500, 1, 'status:test', '...', {})
        self.assertEqual(r[2]['available'], 0)
        # too soon
        r = self.sch.allocate(1,'test', now, 500, 1, 'status:test', '...', {})
        self.assertIsNone(r[0])
        # too soon after the previous experiment
        r = self.sch.allocate(1,'test', now + 1000, 500, 1, 'status:test', '...', {})
        self.assertIsNone(r[0])
        # too short
        r = self.sch.allocate(1,'test', now + 1500, 1, 1, 'status:test', '...', {})
        self.assertIsNone(r[0])
        # zero node count
        r = self.sch.allocate(1,'test', now + 1500, 500, 0, 'status:test', '...', {})
        self.assertIsNone(r[0])
        # too many nodes
        r = self.sch.allocate(1,'test', now + 1500, 500, 2, 'status:test', '...', {})
        self.assertEqual(r[2]['available'], 1)
        # too much storage requested
        r = self.sch.allocate(1,'test', now + 1500, 500, 1, 'status:test', '...',
                              {'storage': 501 * 1000000000})
        self.assertEqual(r[2]['requested'], 501 * 1000000000)

    def test_11_recurrence(self):
        now = int(time.time())
        r = self.sch.allocate(1,'test', now + 1500, 500, 1, 'status:test', '...',
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
        r = self.sch.allocate(1,'test', now + 1500, 500, 1, 'status:test', '...',
                {'recurrence':'simple',
                 'period': 3600,
                 'until': now + 100000000000
                 })
        self.assertLess(r[2]['stop'], now + 100000000000)

        # interleaving recurrence
        r = self.sch.allocate(1,'test', now + 2500, 500, 1, 'status:test', '...',
                {'recurrence':'simple',
                 'period': 3600,
                 'until': now + 2500 + 3600 * 2 + 500
                })
        self.assertEqual(r[2]['nodecount'], 1)
        self.assertEqual(r[2]['intervals'], 3)

        r = self.sch.allocate(1,'test', now + 3500, 500, 1, 'status:test', '...',
                {'recurrence':'simple',
                 'period': 3600,
                 'until': now + 3500 + 3600 * 2 + 500
                })
        self.assertEqual(r[2]['nodecount'], 1)
        self.assertEqual(r[2]['intervals'], 3)

    def test_12_find_slot(self):
        r = self.sch.find_slot(1)
        self.assertEqual(r[0][0]['nodecount'], 1)
        r = self.sch.find_slot(1, nodetypes='status:fail')
        self.assertIsNone(r[0])
        r = self.sch.find_slot(1, duration=500, nodes=['1'],
                               nodetypes='status:test')
        self.assertIsNotNone(r[0])
        self.assertEqual(r[0][0]['nodecount'], 1)

        # actually allocate the suggested slot
        r = self.sch.allocate(1,'test', r[0][0]['start'], 500, 1, 'status:test', '...', {})
        self.assertEqual(r[2]['nodecount'], 1)

        r = self.sch.find_slot(1, duration=500, nodes=['500','600'])
        self.assertIsNone(r[0])

    def test_13_quotas(self):
        now = int(time.time())
        self.sch.set_time_quota(1, 500)
        self.sch.set_data_quota(1, 500)
        self.sch.set_storage_quota(1, 500)
        r = self.sch.allocate(1,'test', now + 500, 600, 1, 'status:test', '...', {})
        self.assertEqual(r[2]['required'], 600)
        r = self.sch.allocate(1,'test', now + 500, 500, 1, 'status:test', '...',
                              {'traffic':200})
        self.assertEqual(r[2]['required'], 600)
        r = self.sch.allocate(1,'test', now + 500, 500, 1, 'status:test', '...',
                              {'storage':600})
        self.assertEqual(r[2]['required'], 600)

    def test_14_journal(self):
        r = self.sch.get_quota_journal(userid=1)
        self.assertEqual(len(r), 39)

    def test_15_reports(self):
        r = self.sch.get_schedule(schedid=1)
        self.assertEqual(r[0]['report'], {})

    def test_21_delete_experiment(self):
        r = self.sch.delete_experiment(99)
        self.assertEqual(r[0],0)
        r = self.sch.delete_experiment(1)
        self.assertEqual(r[0],1)


    def test_99_cleanup(self):
        #os.unlink(TEMP_DB)
        os.unlink(TEMP_LOG)
