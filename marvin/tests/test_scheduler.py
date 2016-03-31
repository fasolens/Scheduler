#!/usr/bin/env python

import unittest
import os
import configuration

TEMP_DB = '/tmp/test_marvin.db'
TEMP_LOG = '/tmp/test_marvin.log'

class SchedulerTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(SchedulerTestCase, self).__init__(*args, **kwargs)

        configuration.select('marvinctld',
                             preset = { 'inventory':{'sync':False},
                                        'database': TEMP_DB,
                                        'log': {'level': 50,
                                                'file': TEMP_LOG}})

        from scheduler import Scheduler
        self.sch = Scheduler()

    def test_00_db_exists(self):
        self.assertTrue(os.path.isfile(TEMP_DB))
        self.assertIsNotNone(self.sch.db())

    def test_01_create_user(self):
        userid, error = self.sch.create_user('admin','admin','admin')
        self.assertIsNotNone(userid)
        user = self.sch.get_users(ssl='admin')
        self.assertIsNotNone(user)
        self.assertEqual(user[0].get('id',None), 1)
        role = self.sch.get_role(ssl='admin')
        self.assertEqual(role, 'admin')

    def test_02_create_node(self):
        c = self.sch.db().cursor()
        c.execute( "INSERT OR IGNORE INTO nodes VALUES (?, ?, ?, ?)",
                   ('1', 'test-node', 'active', 0))
        self.sch.db().commit()
        self.sch.set_node_types(1, 'test')
        result = self.sch.get_nodes(nodetype='test')
        self.assertEqual(result[0].get('id', None), 1)

    def test_03_delete_user(self):
        userid, error = self.sch.create_user('foo','foo','foo')
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

    # TODO: test the actual scheduling

    def test_99_cleanup(self):
        os.unlink(TEMP_DB)
        os.unlink(TEMP_LOG)
