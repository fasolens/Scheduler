#!/usr/bin/env python

import configuration
import datetime
from inventory import inventory_api
from itertools import chain
import logging
from logging.handlers import WatchedFileHandler
import os
import re
import simplejson as json
import sqlite3 as db
import sys
from thread import get_ident
import threading
import traceback
import time

from Crypto.PublicKey import RSA

config = configuration.select('marvinctld')

log = logging.getLogger('Scheduler')
log.addHandler(WatchedFileHandler(config['log']['file']))
log.setLevel(config['log']['level'])



log.debug("Configuration loaded: " + str(config))

DEPLOYMENT_SERVER=config['repository']['deployment']
DEPLOYMENT_RE=re.compile('^(https://)?' + re.escape(DEPLOYMENT_SERVER) + '.*')
ERROR_INSUFFICIENT_RESOURCES = "sc1"
ERROR_PARSING_FAILED = "sc2"

TASK_FINAL_CODES  = [
    'stopped',     # experiment stopped by scheduler
    'finished',    # experiment completed, exited before being stopped
    'failed',      # scheduling process failed
    'canceled',    # user deleted experiment, task not deployed (but some were)
    'aborted',     # user deleted experiment, task had been deployed
]
TASK_STATUS_CODES = TASK_FINAL_CODES + [
    'defined',     # experiment is created in the scheduler
    'requested',   # the scheduling task has been sent to the node
    'deployed',    # node has deployed the experiment, scheduled a start time
    'delayed',     # scheduling process failed temporarily
    'started',     # node has successfully started the experiment
    'restarted',   # node has restarted the experiment after a node failure
]

# POLICY CHECKS AND VALUES
# task may be scheduled # seconds after NOW
POLICY_TASK_DELAY = 60
# task must be scheduled for a minimum of # seconds
POLICY_TASK_MIN_RUNTIME = 60
# task may not run for a maximum of # seconds
POLICY_TASK_MAX_RUNTIME = 25 * 3600
# recurrence may only happen with a minimum period of # seconds
POLICY_TASK_MIN_RECURRENCE = 3600
POLICY_TASK_STEP_RECURRENCE = 3600
# scheduling may only happen # seconds in advance
POLICY_SCHEDULING_PERIOD = 31 * 24 * 3600
# scheduling may only happen # seconds after previous task
POLICY_TASK_PADDING = 2 * 60

# some default quotas until we have something else defined
POLICY_DEFAULT_QUOTA_TIME = 50 * 24 * 3600      # 50 Node days
POLICY_DEFAULT_QUOTA_DATA = 50 * 1000000000     # 50 GB
POLICY_DEFAULT_QUOTA_STORAGE = 50 * 1000000000  # 50 GB
POLICY_DEFAULT_QUOTA_MODEM = 50 * 1000000000    # 50 GB

POLICY_TASK_MAX_STORAGE = 1048576 * 1000        # 1000 MiB per node
POLICY_TASK_MAX_TRAFFIC = 524288 * 1000         # 500 MiB per interface

NODE_MISSING = 'missing'  # existed in the past, but no longer listed
NODE_DISABLED = 'disabled'  # set to STORAGE or other in the inventory
NODE_ACTIVE = 'active'  # set to DEPLOYED (or TESTING) in the inventory
NODE_MAINTENANCE = 'maintenance'  # node is in MAINTENANCE mode

NODE_STATUS_CODES = [NODE_MISSING, NODE_DISABLED,
                     NODE_ACTIVE, NODE_MAINTENANCE]

ROLE_USER = 'user'
ROLE_NODE = 'node'
ROLE_ADMIN = 'admin'
ROLE_INVALID = 'invalid'

NODE_TYPE_STATIC = 'static'
NODE_TYPE_MOBILE = 'mobile'
NODE_TYPE_TESTING = 'testing'
NODE_TYPE_SPECIAL = 'special' # have to be explicitly requested

DEVICE_HISTORIC = 'historic'
DEVICE_CURRENT = 'current'

EXPERIMENT_ACTIVE='active'
EXPERIMENT_ARCHIVED='archived'

LPQ_SCHEDULING = -1

QUOTA_MONTHLY = 0
QUOTA_DAYOFMONTH = 1


AM0930 = 34200
AM1000 = 36000
PM0930 = 77400
PM1000 = 79200
HOURS12 = 43200

last_sync = 0

class SchedulerException(Exception):
    pass


class Scheduler:

    def __init__(self, refresh=False):
        self.check_db(refresh)
        if config.get('inventory', {}).get('sync', True):
            self.sync_inventory()

    def sync_inventory(self):
        last_sync = int(time.time())
        try:
            nodes = inventory_api("nodes/status")
            if not nodes:
                log.warning("No nodes returned from inventory.")
                return
        except:
            log.warning("Inventory synchronization failed.")
            return

        c = self.db().cursor()

        c.execute("UPDATE nodes SET status = ?", (NODE_MISSING,))
        for node in nodes:
            # update if exists
            log.debug(node)
            status = NODE_ACTIVE if node["Status"] == u'DEPLOYED' \
                or node["Status"] == u'TESTING' \
                else NODE_DISABLED
            if node.get("ProjectName") in ["Celerway", "Monroe"]:
                status = NODE_DISABLED
            c.execute(
                "UPDATE nodes SET hostname = ?, status = ? WHERE id = ?",
                (node.get("Hostname"),
                 status,
                 node["NodeId"]))
            c.execute(
                "INSERT OR IGNORE INTO nodes VALUES (?, ?, ?, ?)",
                (node["NodeId"],
                 node.get("Hostname"),
                    status,
                    0))
            types = []

            for key, tag in [('Country', 'country'),
                             ('Model', 'model'),
                             ('ProjectName', 'project'),
                             ('Latitude', 'latitude'),
                             ('Longitude', 'longitude'),
                             ('ProjectName', 'site'),
                             ('Status', 'type')]:
                value = node.get(key)
                if value is not None:
                    types.append((tag, value.lower()))

            c.execute("DELETE FROM node_type WHERE nodeid = ? AND volatile = 1",
                      (node["NodeId"],))
            for tag, type_ in types:
                c.execute(
                    "INSERT OR IGNORE INTO node_type VALUES (?, ?, ?, ?)",
                    (node["NodeId"], tag, type_, 1))

        devices = inventory_api("nodes/devices")
        if not devices:
            log.error("No devices returned from inventory.")
            sys.exit(1)

        c.execute("UPDATE node_interface SET status = ?", (DEVICE_HISTORIC,))
        for device in devices:
            if not device.get('Iccid'):
                continue
            if not device.get('MccMnc'):
                continue

            c.execute("SELECT * from node_interface "
                      "WHERE imei = ? AND iccid = ? AND nodeid = ?",
                      (device.get('DeviceId'), device.get('Iccid'), device.get('NodeId')))
            result = c.fetchall()
            if len(result)>0:
                c.execute("UPDATE node_interface SET status = ? "
                          "WHERE imei = ? AND iccid = ? AND nodeid = ?",
                          (DEVICE_CURRENT, device.get('DeviceId'), device.get('Iccid'), device.get('NodeId')))
            else:
                c.execute("INSERT INTO node_interface "
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (device.get('NodeId'), device.get('DeviceId'),
                           device.get('MccMnc'), device.get('Operator'),
                           device.get('Iccid'),
                           0, 0, QUOTA_MONTHLY, 0, 0, DEVICE_CURRENT, 0))
        self.db().commit()

    connections = {}

    def db(self, refresh=False):
        id = get_ident() or threading.current_thread().ident
        if refresh or not self.connections.get(id):
            log.debug("Connection opened for thread id %s" % id)
            self.connections[id] = db.connect(config['database'],
                                              timeout=30.0)
            self.connections[id].row_factory = db.Row
        return self.connections[id]

    def check_db(self, refresh=False):
        c = self.db(refresh).cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = c.fetchall()
        # TODO: more sanity checks on boot
        if not set(["nodes", "node_type", "node_interface", "owners",
                    "experiments", "schedule",
                    "quota_owner_time", "quota_owner_data",
                    "quota_owner_storage", "quota_journal",
                    "traffic_reports"
                    ]).issubset(set(tables)):
            for statement in """

CREATE TABLE IF NOT EXISTS nodes (id INTEGER PRIMARY KEY ASC,
    hostname TEXT NOT NULL, status TEXT, heartbeat INTEGER);
CREATE TABLE IF NOT EXISTS node_type (nodeid INTEGER NOT NULL,
    tag TEXT NOT NULL, type TEXT NOT NULL, volatile INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (nodeid) REFERENCES nodes(id),
    PRIMARY KEY (nodeid, tag));
CREATE TABLE IF NOT EXISTS node_pair (headid INTEGER NOT NULL,
    tailid INTEGER NOT NULL,
    FOREIGN KEY (headid) REFERENCES nodes(id),
    FOREIGN KEY (tailid) REFERENCES nodes(id));
CREATE TABLE IF NOT EXISTS node_interface (nodeid INTEGER NOT NULL,
    imei TEXT NOT NULL, mccmnc TEXT NOT NULL,
    operator TEXT NOT NULL, iccid TEXT NOT NULL,
    quota_current INTEGER NOT NULL,
    quota_reset_value INTEGER, quota_type INTEGER NOT NULL,
    quota_reset_date  INTEGER, quota_last_reset INTEGER NOT NULL,
    status TEXT NOT NULL, heartbeat INTEGER,
    PRIMARY KEY (nodeid, imei, iccid));
CREATE TABLE IF NOT EXISTS owners (id INTEGER PRIMARY KEY ASC,
    name TEXT UNIQUE NOT NULL, ssl_id TEXT UNIQUE NOT NULL,
    role TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS quota_owner_time (ownerid INTEGER PRIMARY KEY,
    current INTEGER NOT NULL, reset_value INTEGER NOT NULL,
    reset_date INTEGER NOT NULL, last_reset INTEGER,
    FOREIGN KEY (ownerid) REFERENCES owners(id));
CREATE TABLE IF NOT EXISTS quota_owner_data (ownerid INTEGER PRIMARY KEY,
    current INTEGER NOT NULL, reset_value INTEGER NOT NULL,
    reset_date INTEGER NOT NULL, last_reset INTEGER,
    FOREIGN KEY (ownerid) REFERENCES owners(id));
CREATE TABLE IF NOT EXISTS quota_owner_storage (ownerid INTEGER PRIMARY KEY,
    current INTEGER NOT NULL, reset_value INTEGER NOT NULL,
    reset_date INTEGER NOT NULL, last_reset INTEGER,
    FOREIGN KEY (ownerid) REFERENCES owners(id));
CREATE TABLE IF NOT EXISTS experiments (id INTEGER PRIMARY KEY ASC,
    name TEXT NOT NULL, ownerid INTEGER NOT NULL, type TEXT NOT NULL,
    script TEXT NOT NULL, start INTEGER NOT NULL, stop INTEGER NOT NULL,
    recurring_until INTEGER NOT NULL, options TEXT,
    status TEXT,
    FOREIGN KEY (ownerid) REFERENCES owners(id));
CREATE TABLE IF NOT EXISTS schedule (id TEXT PRIMARY KEY ASC,
    nodeid INTEGER, expid INTEGER, start INTEGER, stop INTEGER,
    status TEXT NOT NULL, shared INTEGER, deployment_options TEXT,
    FOREIGN KEY (nodeid) REFERENCES nodes(id),
    FOREIGN KEY (expid) REFERENCES experiments(id));
CREATE TABLE IF NOT EXISTS traffic_reports (schedid TEXT,
    meter TEXT NOT NULL, value INTEGER NOT NULL,
    FOREIGN KEY (schedid) REFERENCES schedule(id));
CREATE UNIQUE INDEX IF NOT EXISTS k_all ON traffic_reports(schedid, meter);
CREATE INDEX IF NOT EXISTS k_iccid      ON node_interface(iccid);
CREATE TABLE IF NOT EXISTS quota_journal (timestamp INTEGER,
    quota TEXT NOT NULL, ownerid INTEGER, iccid TEXT,
    new_value INTEGER NOT NULL,
    reason TEXT NOT NULL,
    FOREIGN KEY (ownerid) REFERENCES owners(id),
    FOREIGN KEY (iccid) REFERENCES node_interface(iccid));
CREATE TABLE IF NOT EXISTS key_pairs (
    private TEXT NOT NULL, public TEXT NOT NULL,
    expires INTEGER NOT NULL);

CREATE INDEX IF NOT EXISTS k_status     ON nodes(status);
CREATE INDEX IF NOT EXISTS k_heartbeat  ON nodes(heartbeat);
CREATE INDEX IF NOT EXISTS k_type       ON node_type(type);
CREATE INDEX IF NOT EXISTS k_ssl_id     ON owners(ssl_id);
CREATE INDEX IF NOT EXISTS k_recurring  ON experiments(recurring_until);
CREATE INDEX IF NOT EXISTS k_start      ON schedule(start);
CREATE INDEX IF NOT EXISTS k_stop       ON schedule(stop);
CREATE INDEX IF NOT EXISTS k_expid      ON schedule(expid);
CREATE INDEX IF NOT EXISTS k_times      ON quota_journal(timestamp);
CREATE INDEX IF NOT EXISTS k_expires    ON key_pairs(expires);

            """.split(";"):
                c.execute(statement.strip())
            self.db().commit()

    def get_nodes(self, nodeid=None, nodetype=None):
        c = self.db().cursor()

        columns = """n.id, n.hostname, n.status, n.heartbeat, t.tag, t.type,
                     i.imei as i_imei, i.mccmnc as i_mccmnc,
                     i.operator as i_operator, i.iccid as i_iccid,
                     i.status as i_status, i.heartbeat as i_heartbeat,
                     i.quota_current as i_quota_current,
                     i.quota_last_reset as i_quota_last_reset,
                     i.quota_reset_value as i_quota_reset_value,
                     i.quota_reset_date as i_quota_reset_date
                  """

        join = "FROM nodes n LEFT JOIN node_type t on n.id=t.nodeid LEFT JOIN node_interface i on n.id=i.nodeid"
        if nodeid is not None:
            c.execute("SELECT %s %s WHERE n.id = ? AND i.status != ?" % (columns, join), (nodeid, DEVICE_HISTORIC))
        elif nodetype is not None:
            tag, type_ = nodetype.split(":")
            c.execute("SELECT %s %s WHERE EXISTS "
                      "(SELECT nodeid FROM node_type "
                      " WHERE tag = ? AND type = ?)" % (columns, join),
                      (tag, type_))
        else:
            c.execute("SELECT %s %s" % (columns, join))
        noderows = [dict(x) for x in c.fetchall()]
        nodes = {}
        for row in noderows:
            id = row['id']
            list_ = nodes.get(id,{})
            tag = None
            for k,v in row.iteritems():
                if k=="tag":
                    tag = v
                elif k=="type":
                    list_[tag]=v
                elif k[0:2] == "i_":
                    if v is not None:
                        ifaces = list_.get("interfaces",{})
                        imei = row['i_imei']
                        iface = ifaces.get(imei,{})
                        iface[k[2:]] = v
                        ifaces[imei] = iface
                        list_["interfaces"] = ifaces
                else:
                    list_[k]=v
            nodes[id]=list_
        nodes = nodes.values()
        for n in nodes:
            n["interfaces"]=n.get("interfaces",{}).values()
        return nodes

    def generate_key_pair(self):
        key = RSA.generate(2048, os.urandom)
        return key.exportKey('PEM'), key.publickey().exportKey('OpenSSH')
    def get_public_keys(self):
        c = self.db().cursor()
        now = int(time.time())
        c.execute("DELETE FROM key_pairs WHERE expires < ?", (now,))
        self.db().commit()
        c.execute("SELECT public FROM key_pairs")
        data = c.fetchall()
        keys = [dict(x)['public'] for x in data]
        return keys

    def set_node_types(self, nodeid, nodetypes):
        c = self.db().cursor()
        types = set(nodetypes.split(","))
        try:
            c.execute("DELETE FROM node_type WHERE nodeid = ? AND volatile = 0", (nodeid,))
            for type_ in types:
                tag, ty = type_.strip().split(":")
                c.execute(
                    "INSERT OR REPLACE INTO node_type VALUES (?, ?, ?, ?)",
                    (nodeid, tag, ty, 0))
            self.db().commit()
            if c.rowcount == 1:
                return True
            else:
                return "Node id not found."
        except db.Error as er:
            log.warning(er.message)
            return "Error updating node."

    def set_node_pair(self, headid, tailid):
        c = self.db().cursor()
        c.execute("SELECT id FROM nodes")
        nodes = [str(x[0]) for x in c.fetchall()]
        if not headid in nodes or (tailid is not None and not tailid in nodes):
            return "Node id does not exist."
        c.execute("DELETE FROM node_pair WHERE headid=? OR tailid=?", (headid, headid))
        if tailid is not None:
            c.execute("DELETE FROM node_pair WHERE headid=? OR tailid=?", (tailid, tailid))
            c.execute("INSERT INTO node_pair VALUES (?,?)", (headid, tailid))
        self.db().commit()
        return c.rowcount


    def check_quotas(self):
        now = int(time.time())
        c = self.db().cursor()
        for table in ['quota_owner_time', 'quota_owner_data',
                      'quota_owner_storage']:
            c.execute("""UPDATE %s SET current = reset_value,
                                       reset_date = ?,
                                       last_reset = ?
                         WHERE reset_date < ?""" % table,
                      (self.first_of_next_month(), now, now))
            if c.rowcount > 0:
                c.execute("""INSERT INTO quota_journal
                    SELECT last_reset, '%s', ownerid, NULL,
                    reset_value, "scheduled reset"
                    FROM %s WHERE last_reset = %s""" % (table, table, now))
        # interface quotas
        c.execute("""UPDATE node_interface SET quota_current = quota_reset_value,
                                               quota_reset_date = ?,
                                               quota_last_reset = ?
                     WHERE quota_reset_date < ?""",
                  (self.first_of_next_month(), now, now))
        self.db().commit()

    def _set_quota(self, userid, value, table):
        c = self.db().cursor()
        now = int(time.time())
        c.execute("UPDATE %s SET current = ? WHERE ownerid = ?" % table,
                  (value, userid))
        c.execute("INSERT INTO quota_journal VALUES (?, ?, ?, NULL, ?, ?)",
                  (now, table, userid, value, "set by API"))
        self.db().commit()
        return c.rowcount

    def set_data_quota(self, userid, value):
        return self._set_quota(userid, value, 'quota_owner_data')

    def set_storage_quota(self, userid, value):
        return self._set_quota(userid, value, 'quota_owner_storage')

    def set_time_quota(self, userid, value):
        return self._set_quota(userid, value, 'quota_owner_time')

    def set_interface_quota(self, nodeid, iccid, options, value):
        c = self.db().cursor()
        now = int(time.time())
        # TODO: handle other quota types, as per the options.
        #       may require database changes
        reset_date = self.first_of_next_month()
        c.execute("""UPDATE node_interface SET
                         quota_current = ?, quota_reset_value = ?,
                         quota_reset_date = ?, quota_last_reset = ?,
                         quota_type = ?
                     WHERE nodeid = ? AND iccid = ?""",
                  (value, value, reset_date, now, QUOTA_MONTHLY, nodeid, iccid))
        count = c.rowcount
        if c.rowcount > 0:
            c.execute("INSERT INTO quota_journal VALUES (?, ?, NULL, ?, ?, ?)",
                      (now, "node_interface", iccid, value, "set by API"))
        self.db().commit()
        return count

    def get_activity(self):
        activity = {}
        now = int(time.time())
        c = self.db().cursor()
        c.execute("SELECT role, count(*) as count FROM owners GROUP BY role")
        users = dict([(x[0],x[1]) for x in c.fetchall()])
        c.execute("SELECT status, count(*) as count FROM nodes GROUP BY status")
        activity["users"]=users
        nodes = dict([(x[0],x[1]) for x in c.fetchall()])
        c.execute("SELECT count(*) as count FROM nodes WHERE heartbeat > ? - 120", (now,))
        nodes["2 min"]=c.fetchone()[0]
        c.execute("SELECT count(*) as count FROM nodes WHERE heartbeat > ? - 300", (now,))
        nodes["5 min"]=c.fetchone()[0]
        c.execute("SELECT count(*) as count FROM nodes WHERE heartbeat > ? - 3600", (now,))
        nodes["1 h"]=c.fetchone()[0]
        c.execute("SELECT count(*) as count FROM nodes WHERE heartbeat > ? - 86400", (now,))
        nodes["1 d"]=c.fetchone()[0]
        c.execute("SELECT id FROM nodes WHERE heartbeat > ? - 2592000 and heartbeat < ? - 86400", (now,now))
        nodes["dropouts 30d-1d"]=[x[0] for x in c.fetchall()]
        c.execute("SELECT id FROM nodes WHERE heartbeat > ? - 86400 and heartbeat < ? - 300", (now,now))
        nodes["dropouts 1d-5min"]=[x[0] for x in c.fetchall()]
        c.execute("SELECT id FROM nodes WHERE status = ?", (NODE_MAINTENANCE,))
        nodes["maintenance"]=[x[0] for x in c.fetchall()]
        activity["resources"]=nodes
        tasks={}
        c.execute("SELECT count(*) as count FROM schedule WHERE start > ?", (now,))
        tasks["future"]=c.fetchone()[0]
        c.execute("SELECT count(*) as count FROM schedule WHERE start < ? AND stop > ?", (now,now))
        tasks["current"]=c.fetchone()[0]
        c.execute("SELECT DISTINCT o.name FROM schedule s, experiments e, owners o WHERE s.expid=e.id AND e.ownerid=o.id AND s.stop > ? - 604800 GROUP BY e.ownerid", (now,))
        distinct=c.fetchall()
        tasks["distinct active users (7d)"]=[x[0] for x in distinct] if distinct is not None else 0;
        activity["schedules"]=tasks
        return activity

    def get_quota_journal(self, userid=None, iccid=None, nodeid=None, maxage=0):
        c = self.db().cursor()
        query = "SELECT ownerid, new_value, quota, reason, timestamp " \
                "FROM quota_journal WHERE timestamp > ?"
        if userid:
            c.execute(query + " AND ownerid=?", (maxage, userid))
        elif iccid:
            c.execute(query + " AND iccid=?", (maxage, iccid))
        elif nodeid:
            c.execute("""SELECT j.iccid, j.new_value, j.reason, j.timestamp
                         FROM quota_journal j, node_interface i WHERE
                         j.iccid = i.iccid AND i.nodeid = ? AND timestamp > ?""",
                      (nodeid, maxage))
        journal = [dict(x) for x in c.fetchall()]
        return journal

    def get_traffic_report(self, schedid):
        c = self.db().cursor()
        c.execute("SELECT meter,value FROM traffic_reports WHERE schedid=?", schedid)
        report = dict([(x[0],x[1]) for x in c.fetchall()]) or None
        return report

    def get_users(self, userid=None, ssl=None):
        self.check_quotas()

        c = self.db().cursor()
        query = """SELECT o.*, t.current as quota_time,
                               d.current as quota_data,
                               s.current as quota_storage
            FROM
            owners o JOIN quota_owner_time t ON t.ownerid = o.id
                     JOIN quota_owner_data d ON d.ownerid = o.id
                     JOIN quota_owner_storage s ON s.ownerid = o.id
                """
        if userid is not None:
            c.execute(query + " where id = ?", (userid,))
        elif ssl is not None:
            c.execute(query + " where ssl_id = ?", (ssl,))
        else:
            c.execute(query)
        userrows = c.fetchall()
        users = [dict(x) for x in userrows] or None
        return users

    def get_role(self, ssl=None):
        if ssl is None:
            return None
        c = self.db().cursor()
        c.execute("SELECT * FROM owners where ssl_id = ?", (ssl,))
        user = c.fetchone()
        if user:
            return user['role']
        else:
            return None

    def first_of_next_month(self):
        today = datetime.date.today()
        yr = today.year
        if today.month == 12:
            yr = yr + 1
        return datetime.datetime(year=yr,
                                 month=(today.month % 12) + 1,
                                 day=1).strftime('%s')

    def first_of_this_month(self):
        today = datetime.date.today()
        return datetime.datetime(year=today.year,
                                 month=(today.month % 12),
                                 day=1).strftime('%s')

    def create_user(self, name, ssl, role):
        c = self.db().cursor()
        now = int(time.time())
        first_of_next_month = self.first_of_next_month()

        try:
            c.execute(
                "INSERT OR REPLACE INTO owners VALUES (NULL, ?, ?, ?)",
                (name, ssl, role))
            userid = c.lastrowid
            c.execute(
                "INSERT OR REPLACE INTO quota_owner_time "
                "VALUES (?, ?, ?, ?, ?)",
                (userid, POLICY_DEFAULT_QUOTA_TIME, POLICY_DEFAULT_QUOTA_TIME,
                 first_of_next_month, now))
            c.execute("INSERT INTO quota_journal VALUES (?, ?, ?, NULL, ?, ?)",
                      (now, "quota_owner_time", userid,
                       POLICY_DEFAULT_QUOTA_TIME, "user created"))
            c.execute(
                "INSERT OR REPLACE INTO quota_owner_data "
                "VALUES (?, ?, ?, ?, ?)",
                (userid, POLICY_DEFAULT_QUOTA_DATA, POLICY_DEFAULT_QUOTA_DATA,
                 first_of_next_month, now))
            c.execute("INSERT INTO quota_journal VALUES (?, ?, ?, NULL, ?, ?)",
                      (now, "quota_owner_data", userid,
                       POLICY_DEFAULT_QUOTA_DATA, "user created"))
            c.execute(
                "INSERT OR REPLACE INTO quota_owner_storage "
                "VALUES (?, ?, ?, ?, ?)",
                (userid, POLICY_DEFAULT_QUOTA_STORAGE,
                 POLICY_DEFAULT_QUOTA_STORAGE, first_of_next_month, now))
            c.execute("INSERT INTO quota_journal VALUES (?, ?, ?, NULL, ?, ?)",
                      (now, "quota_owner_storage", userid,
                       POLICY_DEFAULT_QUOTA_STORAGE, "user created"))
            self.db().commit()
            return userid, None
        except db.Error as er:
            log.warning(er.message)
            return None, "Error inserting user."

    def delete_user(self, userid):
        c = self.db().cursor()
        c.execute("UPDATE owners SET role=? WHERE id = ?", (ROLE_INVALID, userid,))
        self.db().commit()
        if c.rowcount == 1:
            return True
        else:
            return None

    def get_schedule(self, schedid=None, expid=None, nodeid=None,
                     userid=None, past=False, start=0, stop=0, limit=0,
                     private=False, compact=False, interfaces=False,
                     heartbeat=False):
        """Return scheduled jobs.

        Keywords arguments:
        schedid, expid, nodeid, userid -- (int) filter the job list,
          only the first of these arguments is observed.
        past -- (boolean) whether to return past jobs (stop time > now).
          Currently running jobs are always returned.
        start, stop -- (timestamp) return jobs in the given period
        """

        c = self.db().cursor()
        now, start, stop = int(time.time()), int(start), int(stop)
        period = self.get_scheduling_period()
        if start == 0:
            start = now
        if stop == 0:
            stop = period[1]
        if compact:
            selectq = "SELECT s.nodeid, s.start, s.stop, e.ownerid"
        else:
            selectq = "SELECT *"
        pastq = (
                    " AND (NOT (s.start>%i OR s.stop<%i) OR s.start = -1) " % (stop, start)
                ) if not past else ""
        orderq = " ORDER BY s.start ASC"
        if limit > 0:
            orderq += " LIMIT %i" % limit
        if schedid is not None:
            c.execute(
                selectq + " FROM schedule s WHERE s.id = ?" + pastq + orderq, (schedid,))
        elif expid is not None:
            c.execute(
                selectq + " FROM schedule s WHERE s.expid = ?" + pastq + orderq, (expid,))
        elif nodeid is not None:
            c.execute(
                selectq + " FROM schedule s WHERE s.nodeid=?" + pastq + orderq, (nodeid,))
        elif userid is not None:
            c.execute(selectq + " FROM schedule s, experiments t "
                      "WHERE s.expid = t.id AND t.ownerid=?" +
                      pastq + orderq, (userid,))
        else:
            c.execute(selectq + " FROM schedule s, experiments e WHERE s.expid=e.id " + pastq + orderq)
        taskrows = c.fetchall()
        tasks = [dict(x) for x in taskrows]

        # handle LPQ tasks: if start is undefined...
        num_tasks = len(tasks)
        next_tasks = [t for t in tasks if t.get('start') != -1]
        if num_tasks > 0 and tasks[0]['start'] == -1 and heartbeat:
            lpq_task = tasks[0]
            duration = lpq_task['stop']
            # and there is an available time window...
            if len(next_tasks) == 0 or \
               next_tasks[0]['start'] > now + POLICY_TASK_PADDING * 2 + duration:
                   # then set execution time to now.
                   lpq_task['start'] = now + POLICY_TASK_PADDING
                   lpq_task['stop'] = now + POLICY_TASK_PADDING + duration
                   c.execute("UPDATE schedule SET start=?, stop=? WHERE id=?",
                             (lpq_task['start'], lpq_task['stop'], lpq_task['id']))
                   self.db().commit()
                   # and return one LPQ task, before anything scheduled
                   tasks = [lpq_task] + next_tasks
        else:
            # do not return lpq tasks, even if they cannot be scheduled
            tasks = next_tasks

        if compact is False:
            for x in tasks:
                x['deployment_options'] = json.loads(
                    x.get('deployment_options', '{}'))
                if not private:
                    for key in x['deployment_options'].keys():
                        if key[0]=='_':
                            del x['deployment_options'][key]
            if len(tasks)==1:
                for x in tasks:
                    c.execute("SELECT meter,value FROM traffic_reports WHERE schedid=?",
                              (x.get('id'),))
                    x['report']=dict([(r[0],r[1]) for r in c.fetchall()])
        if limit > 0:
            tasks = tasks[:limit]
        if (interfaces is True) and (nodeid is not None):
            c.execute("SELECT iccid, quota_current FROM node_interface where nodeid=?", (nodeid,))
            ifrows = c.fetchall()
            interfaces = [dict(x) for x in ifrows]
            return {"interfaces":interfaces, "tasks":tasks}
        else:
            #FIXME: use dict format for all return values
            return tasks

    def report_traffic(self, schedid, traffic):
        c = self.db().cursor()
        # these two are on the deployment quota
        if 'deployment' in traffic:
          c.execute("INSERT OR REPLACE INTO traffic_reports VALUES (?,?,?)",
                    (schedid, 'deployment', traffic['deployment']))
        if 'results' in traffic:
          c.execute("INSERT OR REPLACE INTO traffic_reports VALUES (?,?,?)",
                    (schedid, 'results', traffic['results']))
        if 'interfaces' in traffic:
          for iccid, value in traffic['interfaces'].iteritems():
              c.execute("INSERT OR REPLACE INTO traffic_reports VALUES (?,?,?)",
                        (schedid, iccid, value))
        if traffic.get('final',False):
            #TODO: restore quotas
            pass
        self.db().commit()
        return True, "Ok."

    def set_status(self, schedid, status):
        c = self.db().cursor()
        code = status.split(';')[0]
        if code in TASK_STATUS_CODES:
            c.execute("SELECT status FROM schedule WHERE id = ?", (schedid,))
            result = c.fetchone()
            if not result:
                return False, "Could not find scheduling ID"
            oldstat = result[0]
            oldcode = oldstat.split(';')[0]
            if oldcode not in TASK_FINAL_CODES:
                c.execute(
                    "UPDATE schedule SET status = ? WHERE id = ?",
                    (status, schedid))
                self.db().commit()
                if c.rowcount == 1:
                    return True, "Ok."
            else:
                return False, "Status %s cannot be reset." % str(oldstat)
        return False, "Unknown status code (%s)." % str(status)

    def get_experiments(self, expid=None, userid=None, nodeid=None, schedid=None, archived=False):
        c = self.db().cursor()
        archq = " AND e.status='%s' " % EXPERIMENT_ACTIVE if not archived else ""
        if expid is not None:
            c.execute(
                "SELECT * FROM experiments e WHERE e.id=?" + archq, (expid,))
        elif userid is not None:
            c.execute("SELECT * FROM experiments e WHERE e.ownerid=?" + archq, (userid,))
        elif nodeid is not None:
            c.execute(
                "SELECT e.id, e.name, e.ownerid, e.type, e.script, e.options, e.status"
                "FROM schedule s, experiments e WHERE s.expid = e.id AND "
                "s.nodeid=?" + archq, (nodeid,))
        else:
            c.execute("SELECT * FROM experiments e WHERE 1==1" + archq)
        taskrows = c.fetchall()
        experiments = [dict(x) for x in taskrows]
        for i, task in enumerate(experiments):
            if schedid is not None:
                if schedid == -1: # return all scheduling results
                    query="SELECT id, nodeid, status, start, stop FROM schedule WHERE expid=?"
                    c.execute(query, (experiments[i]['id'],))
                else:
                    query="SELECT id, nodeid, status, start, stop FROM schedule WHERE expid=? AND id=?"
                    c.execute(query, (experiments[i]['id'], schedid))
                result = [dict(x) for x in c.fetchall()]
                schedules = dict([(
                               x['id'],
                               {"status": x['status'], "nodeid": x['nodeid'],
                                "start": x['start'], "stop": x['stop']}
                              ) for x in result])
                query="SELECT deployment_options FROM schedule WHERE expid=?"
                c.execute(query, (experiments[i]['id'],))
                result = c.fetchone()
                if result is not None:
                    opts=json.loads(result[0])
                    for key in opts.keys():
                        if key[0]=='_':
                            del opts[key]
                    #schedules['deployment_options']=opts
                experiments[i]['schedules'] = schedules
            else:
                query="SELECT status, count(*) FROM schedule WHERE expid=? GROUP BY status"
                c.execute(query, (experiments[i]['id'],))
                result = dict([(x[0],x[1]) for x in c.fetchall()])
                experiments[i]['summary'] = result

            experiments[i]['options'] = json.loads(task.get('options', '{}'))
            for key in experiments[i]['options'].keys():
                if key[0]=='_':
                    del experiments[i]['options'][key]
            if 'recurring_until' in experiments[i]:
                del experiments[i]['recurring_until']
        return experiments or None

    def get_scheduling_period(self):
        """returns the period which we have to check for periodic scheduling
          assuming a prebooking period of 1 month, both maximum and minimum
          are 31 days.
        """
        now = int(time.time())
        return now + POLICY_TASK_DELAY, now + POLICY_SCHEDULING_PERIOD

    def get_recurrence_intervals(self, start, stop, opts):
        recurrence = opts.get('recurrence', None)
        period = int(opts.get('period', 0))
        until = int(opts.get('until', 0))
        now = time.time()

        until = min(self.get_scheduling_period()[1], int(until))

        if start < now + POLICY_TASK_DELAY:
            raise SchedulerException(
                "Tasks may not be scheduled immediately or in the past "
                "(%i second delay)." % POLICY_TASK_DELAY)
        if stop < start + POLICY_TASK_MIN_RUNTIME:
            raise SchedulerException(
                "Tasks must run for a minimum of %i seconds." %
                POLICY_TASK_MIN_RUNTIME)
        if stop > start + POLICY_TASK_MAX_RUNTIME:
            raise SchedulerException(
                "Tasks may not run for more than %i seconds." %
                POLICY_TASK_MAX_RUNTIME)
        if start > now + POLICY_SCHEDULING_PERIOD:
            raise SchedulerException(
                "Tasks may not run be scheduled more than "
                "%s seconds in advance." % POLICY_SCHEDULING_PERIOD)

        if recurrence is None:
            return [(start, stop)]
        elif recurrence == "simple":
            if until < start:
                raise SchedulerException(
                    "End of recurrence set to before start time.")

            delta = stop - start
            if period < delta:
                raise SchedulerException("Recurrence period too small. "
                                         "Must be greater than task runtime.")
            if period < POLICY_TASK_MIN_RECURRENCE:
                raise SchedulerException("Recurrence period too small. "
                                         "Must be greater than %i seconds." %
                                         POLICY_TASK_MIN_RECURRENCE)
            if period % POLICY_TASK_STEP_RECURRENCE != 0:
                raise SchedulerException("Recurrence must be a multiple of "\
                                         "%i seconds." %
                                         POLICY_TASK_STEP_RECURRENCE)
            intervals = [(fro, fro + delta)
                         for fro in xrange(start, until, period)]
            return intervals
        else:
            raise SchedulerException(
                "Unsupported recurrence scheme")

    def is_maintenance(self, start, stop):
        """return True if timestamp is between 9:30 to 10:00 am/pm"""

        duration = stop-start
        tod = start % 86400
        fin = tod + duration
        if (tod < AM1000) and (fin > AM0930):
            return True
        if (tod < PM1000) and (fin > PM0930):
            return True
        return False

    def segments_maintenance(self, start, stop):
        """return all timestamps between start and stop that match a mainenance
           window boundary"""

        duration = stop-start
        tod = start % 86400
        day = start - tod

        segments = []
        for t in xrange(day + AM0930, stop, HOURS12):
            if (t>start and t<stop):
                segments.append(t)
        for t in xrange(day + AM1000, stop, HOURS12):
            if (t>start and t<stop):
                segments.append(t)
        return segments


    def get_available_nodes(self, nodes, type_require,
                            type_reject, start, stop,
                            head=True, tail=False, pair=False):
        """ Select all active nodes not having a task scheduled between
            start and stop from the set of nodes matching type_accept and
            not type_reject
        """
        # TODO: take node_interface quota into account

        # return empty for overlap with maintenance window
        if start != -1 and self.is_maintenance(start, stop):
            return [], []

        # sync with inventory once per hour
        now = int(time.time())
        if now - last_sync > 3600:
            self.sync_inventory()

        c = self.db().cursor()

        preselection = ""
        if nodes is not None and len(nodes) > 0:
            preselection = " AND n.id IN ('" + "', '".join(nodes) + "') \n"

        query = "\nSELECT n.id AS id, MIN(i.quota_current) AS min_quota"\
                "  FROM nodes n, node_interface i \n"\
                "  WHERE n.status = ? AND n.id = i.nodeid \n"
        query += preselection
        type_require = [x[0].split(":") for x in type_require]
        type_reject = [x[0].split(":") for x in type_reject]
        for type_and in type_require:
            query += "  AND n.id IN (SELECT nodeid FROM node_type " \
                     "  WHERE tag = ? AND type = ?)"
        for type_and in type_reject:
            query += "  AND n.id NOT IN (SELECT nodeid FROM node_type " \
                     "  WHERE tag = ? AND type = ?)"
        if start != -1:
            query += """
AND n.id NOT IN (
    SELECT DISTINCT nodeid FROM schedule s
    WHERE shared = 0 AND NOT ((s.stop + ? < ?) OR (s.start - ? > ?))
)
AND n.heartbeat > ? """

        query += """
GROUP BY n.id
ORDER BY min_quota DESC, n.heartbeat DESC
                 """
        now = int(time.time())

        alive_after = now - 48 * 3600
        if (abs(start-now) < 1200) or (start==0):
            # short heartbeat filter for immediate starts
            alive_after = now - 600
        if start == -1 or nodes is not None:
            # do not apply heartbeat filter on preselection
            alive_after = 0

        parameters = [NODE_ACTIVE] + list(chain.from_iterable(type_require)) + \
                                     list(chain.from_iterable(type_reject)) 
        if start != -1:
            parameters += [POLICY_TASK_PADDING, start, POLICY_TASK_PADDING, stop]
            parameters += [alive_after]
        c.execute(query, parameters)

        noderows = c.fetchall()
        nodes = [x[0] for x in noderows if x[1] is not None]

        c.execute("SELECT * from node_pair") # TODO: cache this, when calling sync
        pairrows = c.fetchall()
        heads = dict(pairrows)

        if pair:
            headn = filter(lambda x: x in heads and heads[x] in nodes, nodes)
            tailn = [heads[x] for x in headn]
            return headn, tailn    # sorted
        else:
            if tail is False:
                tails = {x[1]:x[0] for x in pairrows}
                nodes = filter(lambda x: x not in tails, nodes)
            if head is False:
                nodes = filter(lambda x: x not in heads, nodes)
            return nodes, []


    def parse_node_types(self, nodetypes=""):
        try:
            types = nodetypes.split(",")
            type_reject = [t[1:].strip() for t in types
                           if len(t) > 2 and t[0] == '-']
            type_require = [t.strip() for t in types
                            if len(t) > 1 and t[0] != '-']
            return [t.split("|") for t in type_require],\
                   [t.split("|") for t in type_reject]
        except Exception, ex:
            return None, "nodetype expression could not be parsed. "+ex.message

    def find_slot(self, nodecount=1, duration=1, start=0,
                  nodetypes="", nodes=None, results=1,
                  head=True, tail=False, pair=False):
        """find the next available slot given certain criteria"""

        start, duration, nodecount = int(start), int(duration), int(nodecount)
        period = self.get_scheduling_period()
        start = max(start, period[0])
        stop = period[1]

        if start > period[1]:
            return None, "Provided start time is outside the allowed"\
                         "scheduling range (%s, %s)" % (period)

        selection = nodes

        type_require, type_reject = self.parse_node_types(nodetypes)
        if type_require is None:
            error_message = type_reject
            return "None", error_message

        # fetch all schedule segmentations (experiments starting or stopping)
        c = self.db().cursor()
        where = "WHERE 1==1"
        if selection is not None:
            where += " AND nodeid IN ('" + "', '".join(nodes) + "') \n"
        type_require_ = [x[0].split(":") for x in type_require]
        type_reject_ = [x[0].split(":") for x in type_reject]

        for type_and in type_require:
            where += "  AND nodeid IN (SELECT nodeid FROM node_type " \
                     "  WHERE tag = ? AND type = ?)"
        for type_and in type_reject:
            where += "  AND nodeid NOT IN (SELECT nodeid FROM node_type " \
                     "  WHERE tag = ? AND type = ?)"
        query = """
SELECT DISTINCT * FROM (
    SELECT start - ? AS t FROM schedule %s UNION
    SELECT stop + ?  AS t FROM schedule %s
) WHERE t >= ? AND t < ? ORDER BY t ASC;
                """ % (where, where)

        c.execute(query, [POLICY_TASK_PADDING + 1] +
                  list(chain.from_iterable(type_require_)) +
                  list(chain.from_iterable(type_reject_)) +
                  [POLICY_TASK_PADDING + 1] +
                  list(chain.from_iterable(type_require_)) +
                  list(chain.from_iterable(type_reject_)) +
                  [start, stop])
        segments = self.segments_maintenance(start, stop) + \
                   [start] + [x[0] for x in c.fetchall()] + [stop]
        segments.sort()

        slots = []

        while len(segments) > 1:
            s0 = segments[0]
            c = 1

            while segments[c]-s0 < duration:
                if c == len(segments):
                    return None, "Could not find available time slot "\
                                 "matching these criteria."
                c += 1

            nodes, tails = self.get_available_nodes(
                               selection,
                               type_require, type_reject, s0, segments[c],
                               head, tail, pair)
            if len(nodes) >= nodecount:
                slots.append({
                    'start': s0,
                    'stop': s0 + duration,
                    'max_stop': segments[c],
                    'nodecount': nodecount,
                    'max_nodecount': len(nodes),
                    'nodetypes': nodetypes,
                })
                if len(slots) >= results:
                    return slots, None
            # TODO: ideally, we identify the segment where the conflict occurs
            # and skip until after this segment. until then, we just iterate.

            segments.pop(0)

        if len(slots) > 0:
            return slots, None
        else:
            return None, "Could not find available time slot "\
                         "matching these criteria."

    def allocate(self, user, name, start, duration, nodecount,
                 nodetypes, scripts, options,
                 head=True, tail=False, pair=False):
        """Insert a new task on one or multiple nodes,
        creating one or multible jobs.

        Keyword arguments:
        user        -- userid of the experimenter
        name        -- arbitrary identifier
        start       -- unix time stamp (UTC)
        duration    -- duration of the experiment in seconds
        nodecount   -- number of required nodes.
        nodetypes   -- filter on node type (static,spain|norway,-apu1)
        scripts     -- deployment URL to be fetched (1 or 2)
        for the extra options, see README.md
        options     -- shared=1 (default 0)
                    -- recurrence, period, until
                    -- storage
                    -- traffic - bidi traffic per interface
                    -- nodes (list of node ids)
                    -- interfaces
                    -- restart
        """

        try:
            start, duration = int(start), int(duration)
            nodecount = int(nodecount)
            assert nodecount > 0
        except Exception as ex:
            return None, "Start time and duration must be in integer seconds "\
                         "(unix timestamps), nodecount an integer > 0", {
                             "code": ERROR_PARSING_FAILED
                         }

        if len(scripts)>2:
            return None, "Only two scripts allowed in script parameter", {
                             "code": ERROR_PARSING_FAILED
                         }

        c = self.db().cursor()
        # confirm userid
        u = self.get_users(userid=user)
        if u is None:
            return None, "Unknown user.", {}
        u = u[0]
        ownerid = u['id']

        try:
            opts = options if type(options) is dict else json.loads(options)
        except:
            try:
                opts = dict([opt.split("=")
                             for opt in options.split("&")]) if options else {}
            except Exception as ex:
                return None,\
                       "options string could not be parsed. "+ex.message,\
                       {"code": ERROR_PARSING_FAILED}

        shared = 1 if opts.get('shared', 0) else 0

        preselection = None
        if opts.get(u"nodes") is not None:
            preselection = opts.get("nodes").split(",")

        if opts.get('internal') is not None and \
          u.get('ssl_id') != "c0004c4c44b2adc8a63d0b5ca62a7acd973198ba":
            return None, "option internal not allowed", {}

        ssh = 'ssh' in opts

        hidden_keys = [
            'recurrence',
            'period',
            'until']
        scheduling_keys = [
            'traffic',
            'shared',
            'recurrence',
            'period',
            'until',
            'storage']
        # pass (almost) all provided parameters to container
        deployment_opts = dict([(key, opts.get(key, None))
                               for key in opts
                               if key not in hidden_keys])
        opts = dict([(key, opts.get(key, None))
                    for key in scheduling_keys if key in opts])

        req_storage = int(opts.get('storage', 0))
        req_traffic = int(opts.get('traffic', 0))

        if req_storage > POLICY_TASK_MAX_STORAGE:
            return None, "Too much storage requested.", \
                   {'max_storage': POLICY_TASK_MAX_STORAGE,
                    'requested': req_storage}
        if req_traffic > POLICY_TASK_MAX_TRAFFIC:
            return None, "Requested data quota too high.", \
                   {'max_data': POLICY_TASK_MAX_TRAFFIC,
                    'requested': req_traffic}

        type_require, type_reject = self.parse_node_types(nodetypes)
        if type_require is None:
            error_message = type_reject
            return None, error_message, {}
        for script in scripts:
            if DEPLOYMENT_RE.match(script) is None:
                if ['type:deployed'] in type_require:
                    return None, "Deployed nodes can only schedule experiments " \
                                 "hosted by %s" % DEPLOYMENT_SERVER, {}
                else:
                    type_reject.append(['type:deployed'])

        if start == 0:
            start = self.get_scheduling_period()[0] + 10
        stop = start + duration

        # LPQ scheduling: start when node is available, no pre-deployment
        # TODO: preserve duration
        lpq = False
        if start == LPQ_SCHEDULING:  # -1
            lpq = True
            stop = -1
            intervals = [(-1, duration)]

        else:
            try:
                intervals = self.get_recurrence_intervals(start, stop, opts)
                if len(intervals)<1:
                    return None, "Something unexpected happened "\
                                 "(no intervals generated)", {}
            except SchedulerException as ex:
                return None, ex.message, {}

        until = int(opts.get('until', 0))

        num_intervals = len(intervals)
        total_time = duration * nodecount * num_intervals
        total_storage = req_storage * nodecount * num_intervals
        total_traffic = req_traffic * nodecount * 3 * num_intervals

        if u['quota_time'] < total_time:
            return None, "Insufficient time quota.", \
                   {'quota_time': u['quota_time'],
                    'required': total_time}
        if u['quota_storage'] < total_storage:
            return None, "Insufficient storage quota.", \
                   {'quota_storage': u['quota_storage'],
                    'required': total_storage}
        if u['quota_data'] < total_traffic:
            return None, "Insufficient data quota.", \
                   {'quota_data': u['quota_data'],
                    'required': total_traffic}

        if len(scripts) == 2:
            pair = True
        elif pair:
            scripts = scripts * 2
        apucount = nodecount
        if pair and nodecount % 2 != 0:
            return None, "Node count must be even for paired nodes.", {}
        elif pair:
            nodecount = nodecount / 2
        node_or_pairs = "node pairs" if pair else "nodes"


        def insert_task (node, script, keypairs):
            deployment_opts['script'] = script
            if keypairs:
                private, public = keypairs.pop()
                deployment_opts['_ssh.private'] = private
                deployment_opts['ssh.public'] = public
            c.execute("INSERT INTO schedule VALUES "
                      "(NULL, ?, ?, ?, ?, ?, ?, ?)",
                      (node, expid, i[0], i[1], 'defined',
                       shared, json.dumps(deployment_opts)))
            if keypairs:
                c.execute("INSERT INTO key_pairs VALUES "
                          "(?, ?, ?)", (private, public, i[1]))

        try:
            available={}
            avl_tails={}
            for inum, i in enumerate(intervals):
                nodes, tails = self.get_available_nodes(
                                   preselection, type_require, type_reject,
                                   i[0], i[1], head=head, tail=tail, pair=pair)

                if len(nodes) < nodecount:
                    self.db().rollback()
                    if i[0] == -1:
                        msg = "This node is not available for scheduling." 
                    else:
                        utcstart = datetime.datetime.utcfromtimestamp(int(i[0])).isoformat()
                        utcstop  = datetime.datetime.utcfromtimestamp(int(i[1])).isoformat()
                        msg = "Only %s/%s %s are available during " \
                              "interval %s (%s to %s)." % \
                              (len(nodes), nodecount, node_or_pairs, inum + 1, utcstart, utcstop)
                    data = {"code": ERROR_INSUFFICIENT_RESOURCES,
                            "available": len(nodes),
                            "requested": nodecount,
                            "selection": preselection,
                            "start": i[0],
                            "stop": i[1]}
                    return None, msg, data
                nodes = nodes[:nodecount]
                tails = tails[:nodecount]
                available[i]=nodes
                avl_tails[i]=tails

            keypairs = [self.generate_key_pair() for x in xrange(apucount * len(intervals))] if ssh else None
            now = int(time.time())

            # no write queries until this point
            c.execute("INSERT INTO experiments "
                      "VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (name, ownerid, nodetypes, "|".join(scripts), start, stop,
                       until, json.dumps(opts), EXPERIMENT_ACTIVE))
            expid = c.lastrowid
            for inum, i in enumerate(intervals):
                for node in available[i]:
                    insert_task(node, scripts[0], keypairs)
                for node in avl_tails[i]:
                    insert_task(node, scripts[1], keypairs)

                # set scheduling ID for all inserted rows, append suffix
                c.execute("UPDATE schedule SET id = ROWID || ? "\
                          "WHERE expid = ?", (config.get('suffix',''), expid))

            c.execute("UPDATE quota_owner_time SET current = ? "
                          "WHERE ownerid = ?",
                          (u['quota_time'] - total_time, ownerid))
            c.execute("""INSERT INTO quota_journal SELECT ?, "quota_owner_time",
                             ownerid, NULL, current,
                             "experiment #%s requested %i seconds runtime (%i %s, %i intervals)" FROM
                             quota_owner_time WHERE ownerid = ?""" % (expid, total_time, nodecount, node_or_pairs, num_intervals),
                             (now, ownerid))

            c.execute("UPDATE quota_owner_storage SET current = ? "
                          "WHERE ownerid = ?",
                          (u['quota_storage'] - total_storage, ownerid))
            c.execute("""INSERT INTO quota_journal SELECT ?, "quota_owner_storage",
                             ownerid, NULL, current,
                             "experiment #%s requested %i bytes (%i %s, %i intervals)" FROM
                             quota_owner_storage WHERE ownerid = ?""" % (expid, total_storage, nodecount, node_or_pairs, num_intervals),
                             (now, ownerid))

            c.execute("UPDATE quota_owner_data SET current = ? "
                          "WHERE ownerid = ?",
                          (u['quota_data'] - total_traffic, ownerid))
            c.execute("""INSERT INTO quota_journal SELECT ?, "quota_owner_data",
                             ownerid, NULL, current,
                             "experiment #%s requested %i bytes (%i %s, %i intervals)" FROM
                             quota_owner_data WHERE ownerid = ?""" % (expid, total_traffic, nodecount, node_or_pairs, num_intervals),
                             (now, ownerid))
            self.db().commit()
            return expid, "Created experiment %s on %s %s " \
                          "as %s intervals." % \
                          (expid, len(nodes), node_or_pairs, len(intervals)), {
                            "experiment": expid,
                            "nodecount": len(nodes),
                            "intervals": len(intervals)
                          }
        except db.Error as er:
            # NOTE: automatic rollback is triggered in case of an exception
            log.error(er.message)
            return None, "Task creation failed.", {'error': er.message}

    def delete_experiment(self, expid):
        c = self.db().cursor()
        c.execute("SELECT DISTINCT status FROM schedule WHERE expid = ?",
                  (expid,))
        result = c.fetchall()
        if len(result) == 0:
            return 0, "Could not find experiment id %s." % expid, {}
        statuses = set([x[0].split(';')[0] for x in result])
        if statuses.issubset(set(['defined','canceled'])):
            c.execute("DELETE FROM schedule WHERE expid = ?", (expid,))
            c.execute("DELETE FROM experiments WHERE id = ?", (expid,))
            self.db().commit()
            return 1, "Ok. Deleted experiment and scheduling entries", {}
        elif statuses.issubset(set(['stopped', 'finished', 'failed', 'canceled', 'aborted'])):
            c.execute("UPDATE experiments SET status=? WHERE id=?", (EXPERIMENT_ARCHIVED, expid))
            self.db().commit()
            return 1, "Ok. Archived experiment.", {}
        else:
            c.execute("""
UPDATE schedule SET status = ? WHERE
    expid = ? AND
    status IN ('defined')
                      """, ('canceled', expid))
            canceled = c.rowcount
            c.execute("""
UPDATE schedule SET status = ? WHERE expid = ? AND
    status IN ('deployed', 'requested', 'started', 'delayed', 'redeployed', 'restarted', 'running') OR status LIKE 'delayed%'
                      """, ('aborted', expid))
            aborted = c.rowcount
            self.db().commit()
            return 1, "Ok. Canceled or aborted open scheduling entries", {
                       "canceled": canceled,
                       "aborted": aborted
                   }

    def update_node_status(self, nodeid, seen, maintenance, interfaces):
        c = self.db().cursor()

        status = NODE_MAINTENANCE if maintenance == '1' else NODE_ACTIVE
        c.execute("UPDATE nodes SET heartbeat=?, status=? where id=? and status!=? and status!=?", (seen, status, nodeid, NODE_DISABLED, NODE_MISSING))
        for iface in interfaces:
            iccid = iface.get('iccid',0)
            host  = int(iface.get('host') or 0)
            netns = int(iface.get('netns') or 0)
            quota = host
            c.execute("UPDATE node_interface SET heartbeat=?, quota_current=quota_reset_value-? where iccid=?", (seen, quota, iccid))
        self.db().commit()

    def update_entry(self, schedid, status):
        c = self.db().cursor()
        c.execute("UPDATE schedule SET status=? where id=?", (status, schedid))
        self.db().commit()
        return c.rowcount
