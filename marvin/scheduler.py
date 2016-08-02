#!/usr/bin/env python

import configuration
import datetime
from inventory import inventory_api
from itertools import chain
import logging
from logging.handlers import WatchedFileHandler
import re
import simplejson as json
import sqlite3 as db
import sys
from thread import get_ident
import threading
import time

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
    'deployed',    # node has deployed the experiment, scheduled a start time
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

POLICY_TASK_MAX_STORAGE = 500 * 1000000              # 500 MB per node
POLICY_TASK_MAX_TRAFFIC = 500 * 1000000              # 500 MB per node TODO

NODE_MISSING = 'missing'  # existed in the past, but no longer listed
NODE_DISABLED = 'disabled'  # set to STORAGE or other in the inventory
NODE_ACTIVE = 'active'  # set to DEPLOYED (or TESTING) in the inventory

NODE_STATUS_CODES = [NODE_MISSING, NODE_DISABLED, NODE_ACTIVE]

ROLE_USER = 'user'
ROLE_NODE = 'node'
ROLE_ADMIN = 'admin'
ROLE_INVALID = 'invalid'

NODE_TYPE_STATIC = 'static'
NODE_TYPE_MOBILE = 'mobile'
NODE_TYPE_TESTING = 'testing'

DEVICE_HISTORIC = 'historic'
DEVICE_CURRENT = 'current'

QUOTA_MONTHLY = 0


class SchedulerException(Exception):
    pass


class Scheduler:

    def __init__(self, refresh=False):
        self.check_db(refresh)
        if config.get('inventory', {}).get('sync', True):
            self.sync_inventory()

    def sync_inventory(self):
        nodes = inventory_api("nodes/status")
        if not nodes:
            log.error("No nodes returned from inventory.")
            sys.exit(1)

        c = self.db().cursor()

        c.execute("UPDATE nodes SET status = ?", (NODE_MISSING,))
        for node in nodes:
            # update if exists
            log.debug(node)
            status = NODE_ACTIVE if node["Status"] == u'DEPLOYED' \
                or node["Status"] == u'TESTING' \
                else NODE_DISABLED
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

            c.execute("DELETE FROM node_type WHERE nodeid = ?",
                      (node["NodeId"],))
            for tag, type_ in types:
                c.execute(
                    "INSERT OR IGNORE INTO node_type VALUES (?, ?, ?)",
                    (node["NodeId"], tag, type_))

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
            c.execute("UPDATE node_interface SET status = ? "
                      "WHERE imei = ?",
                      (DEVICE_CURRENT, node.get('DeviceId'),))
            c.execute("INSERT OR REPLACE INTO node_interface "
                      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (device.get('NodeId'), device.get('DeviceId'),
                       device.get('MccMnc'), device.get('Operator'),
                       device.get('Iccid'),
                       0, 0, QUOTA_MONTHLY, 0, DEVICE_CURRENT))
        self.db().commit()

    connections = {}

    def db(self, refresh=False):
        id = get_ident() or threading.current_thread().ident
        if refresh or not self.connections.get(id):
            log.debug("Connection opened for thread id %s" % id)
            self.connections[id] = db.connect(config['database'])
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
                    "quota_owner_storage" 
                    ]).issubset(set(tables)):
            for statement in """

CREATE TABLE IF NOT EXISTS nodes (id INTEGER PRIMARY KEY ASC,
    hostname TEXT NOT NULL, status TEXT, heartbeat INTEGER);
CREATE TABLE IF NOT EXISTS node_type (nodeid INTEGER NOT NULL,
    tag TEXT NOT NULL, type TEXT NOT NULL,
    FOREIGN KEY (nodeid) REFERENCES nodes(id),
    PRIMARY KEY (nodeid, tag));
CREATE TABLE IF NOT EXISTS node_interface (nodeid INTEGER NOT NULL,
    imei TEXT NOT NULL, mccmnc TEXT NOT NULL,
    operator TEXT NOT NULL, iccid TEXT NOT NULL,
    quota_value INTEGER NOT NULL,
    quota_reset INTEGER, quota_type INTEGER NOT NULL,
    quota_reset_date INTEGER, status TEXT NOT NULL,
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
    FOREIGN KEY (ownerid) REFERENCES owners(id));
CREATE TABLE IF NOT EXISTS schedule (id INTEGER PRIMARY KEY ASC,
    nodeid INTEGER, expid INTEGER, start INTEGER, stop INTEGER,
    status TEXT NOT NULL, shared INTEGER, deployment_options TEXT,
    FOREIGN KEY (nodeid) REFERENCES nodes(id),
    FOREIGN KEY (expid) REFERENCES experiments(id));

CREATE INDEX IF NOT EXISTS k_status     ON nodes(status);
CREATE INDEX IF NOT EXISTS k_heartbeat  ON nodes(heartbeat);
CREATE INDEX IF NOT EXISTS k_type       ON node_type(type);
CREATE INDEX IF NOT EXISTS k_ssl_id     ON owners(ssl_id);
CREATE INDEX IF NOT EXISTS k_recurring  ON experiments(recurring_until);
CREATE INDEX IF NOT EXISTS k_start      ON schedule(start);
CREATE INDEX IF NOT EXISTS k_stop       ON schedule(stop);

            """.split(";"):
                c.execute(statement.strip())
            self.db().commit()

    def get_nodes(self, nodeid=None, nodetype=None):
        c = self.db().cursor()
        if nodeid is not None:
            c.execute("SELECT * FROM nodes WHERE id = ?", (nodeid,))
        elif nodetype is not None:
            tag, type_ = nodetype.split(":")
            c.execute("SELECT * FROM nodes WHERE EXISTS "
                      "(SELECT nodeid FROM node_type "
                      " WHERE tag = ? AND type = ?)",
                      (tag, type_))
        else:
            c.execute("SELECT * FROM nodes")
        noderows = c.fetchall()
        nodes = [dict(x) for x in noderows] or None
        if nodes:
            for node in nodes:
                c.execute(
                    "SELECT tag, type FROM node_type WHERE nodeid = ?",
                    (node.get('id'),))
                typerows = c.fetchall()
                node.update(dict([(row['tag'], row['type'])
                                  for row in typerows]))
                c.execute("SELECT * FROM node_interface WHERE nodeid = ?",
                          (node.get('id'),))
                interfaces = c.fetchall()
                node['interfaces'] = [dict(x) for x in interfaces] or []
        return nodes

    def set_node_types(self, nodeid, nodetypes):
        c = self.db().cursor()
        types = set(nodetypes.split(","))
        try:
            c.execute("DELETE FROM node_type WHERE nodeid = ?", (nodeid,))
            for type_ in types:
                tag, ty = type_.strip().split(":")
                c.execute(
                    "INSERT INTO node_type VALUES (?, ?, ?)",
                    (nodeid, tag, ty))
            self.db().commit()
            if c.rowcount == 1:
                return True
            else:
                return "Node id not found."
        except db.Error as er:
            log.warning(er.message)
            return "Error updating node."

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
        self.db().commit()

    def _set_quota(self, userid, value, table):
        c = self.db().cursor()
        c.execute("UPDATE %s SET current = ? WHERE ownerid = ?" % table,
                  (value, userid))
        self.db().commit()
        return c.rowcount

    def set_data_quota(self, userid, value):
        return self._set_quota(userid, value, 'quota_owner_data')

    def set_storage_quota(self, userid, value):
        return self._set_quota(userid, value, 'quota_owner_storage')

    def set_time_quota(self, userid, value):
        return self._set_quota(userid, value, 'quota_owner_time')

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
        return datetime.datetime(year=today.year,
                                 month=(today.month % 12) + 1,
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
            c.execute(
                "INSERT OR REPLACE INTO quota_owner_data "
                "VALUES (?, ?, ?, ?, ?)",
                (userid, POLICY_DEFAULT_QUOTA_DATA, POLICY_DEFAULT_QUOTA_DATA,
                 first_of_next_month, now))
            c.execute(
                "INSERT OR REPLACE INTO quota_owner_storage "
                "VALUES (?, ?, ?, ?, ?)",
                (userid, POLICY_DEFAULT_QUOTA_STORAGE,
                 POLICY_DEFAULT_QUOTA_STORAGE, first_of_next_month, now))
            self.db().commit()
            return userid, None
        except db.Error as er:
            log.warning(er.message)
            return None, "Error inserting user."

    def delete_user(self, userid):
        c = self.db().cursor()
        c.execute("""DELETE FROM schedule WHERE expid IN (
                   SELECT id FROM experiments WHERE ownerid = ?
              )""", (userid,))
        c.execute("DELETE FROM experiments WHERE ownerid = ?", (userid,))
        c.execute("DELETE FROM owners WHERE id = ?", (userid,))
        self.db().commit()
        if c.rowcount == 1:
            return True
        else:
            return None

    def get_schedule(self, schedid=None, expid=None, nodeid=None,
                     userid=None, past=False, start=0, stop=0, limit=0):
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
        pastq = (
                    " AND NOT (s.start>%i OR s.stop<%i)" % (stop, start)
                ) if not past else ""
        orderq = " ORDER BY s.start ASC"
        if schedid is not None:
            c.execute(
                "SELECT * FROM schedule s WHERE s.id = ?" + pastq + orderq, (schedid,))
        elif expid is not None:
            c.execute(
                "SELECT * FROM schedule s WHERE s.expid = ?" + pastq + orderq, (expid,))
        elif nodeid is not None:
            c.execute(
                "SELECT * FROM schedule s WHERE s.nodeid=?" + pastq + orderq, (nodeid,))
        elif userid is not None:
            c.execute("SELECT * FROM schedule s, experiments t "
                      "WHERE s.expid = t.id AND t.ownerid=?" +
                      pastq + orderq, (userid,))
        else:
            c.execute("SELECT * FROM schedule s WHERE 1=1" + pastq + orderq)
        taskrows = c.fetchall()
        tasks = [dict(x) for x in taskrows]
        for x in tasks:
            x['deployment_options'] = json.loads(
                x.get('deployment_options', '{}'))
        if limit > 0:
            return tasks[:limit]
        else:
            return tasks

    def report_traffic(self, schedid, traffic):
        # TODO
        return False, "Not implemented."

    def set_status(self, schedid, status):
        c = self.db().cursor()
        if status in TASK_STATUS_CODES:
            c.execute("SELECT status FROM schedule WHERE id = ?", (schedid,))
            result = c.fetchone()
            if not result:
                return False, "Could not find scheduling ID"
            oldstat = result[0]
            if oldstat not in TASK_FINAL_CODES:
                c.execute(
                    "UPDATE schedule SET status = ? WHERE id = ?",
                    (status, schedid))
                self.db().commit()
                if c.rowcount == 1:
                    return True, "Ok."
            else:
                return False, "Status %s cannot be reset." % str(oldstat)
        return False, "Unknown status code %s." % str(status)

    def get_experiments(self, expid=None, userid=None, nodeid=None):
        c = self.db().cursor()
        if expid is not None:
            c.execute(
                "SELECT * FROM experiments WHERE experiments.id=?", (expid,))
        elif userid is not None:
            c.execute("SELECT * FROM experiments WHERE ownerid=?", (userid,))
        elif nodeid is not None:
            c.execute(
                "SELECT t.id, t.name, t.ownerid, t.type, t.script, t.options "
                "FROM experiments t, schedule s WHERE s.expid = t.id AND "
                "s.nodeid=?", (nodeid,))
        else:
            c.execute("SELECT * FROM experiments")
        taskrows = c.fetchall()
        experiments = [dict(x) for x in taskrows]
        for i, task in enumerate(experiments):
            c.execute(
                "SELECT id, nodeid, status, start, stop FROM schedule WHERE expid=?",
                (experiments[i]['id'],
                 ))
            result = [dict(x) for x in c.fetchall()]
            schedules = dict([(
                               x['id'],
                               {"status": x['status'], "nodeid": x['nodeid'],
                                "start": x['start'], "stop": x['stop']}
                              ) for x in result])
            experiments[i]['schedules'] = schedules
            experiments[i]['options'] = json.loads(task.get('options', '{}'))
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

    def get_available_nodes(self, nodes, type_require,
                            type_reject, start, stop):
        """ Select all active nodes not having a task scheduled between
            start and stop from the set of nodes matching type_accept and
            not type_reject
        """
        # TODO: take node_interface quota into account

        c = self.db().cursor()

        preselection = ""
        if nodes is not None and len(nodes) > 0:
            preselection = " AND n.id IN ('" + "', '".join(nodes) + "') \n"

        query = "\nSELECT DISTINCT n.id AS id, MIN(i.quota_value) AS min_quota"\
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
        query += """
AND n.id NOT IN (
    SELECT DISTINCT nodeid FROM schedule s
    WHERE shared = 0 AND NOT ((s.stop + ? < ?) OR (s.start - ? > ?))
)
ORDER BY min_quota DESC, n.heartbeat DESC
                 """
        c.execute(query, [NODE_ACTIVE] +
                  list(chain.from_iterable(type_require)) +
                  list(chain.from_iterable(type_reject)) +
                  [POLICY_TASK_PADDING, start, POLICY_TASK_PADDING, stop])

        noderows = c.fetchall()
        nodes = [dict(x) for x in noderows if x[1] is not None]
        return nodes

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
                  nodetypes="", nodes=None, results=1):
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
        segments = [start] + [x[0] for x in c.fetchall()] + [stop]
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

            nodes = self.get_available_nodes(
                        selection,
                        type_require, type_reject, s0, segments[c])
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
                 nodetypes, script, options):
        """Insert a new task on one or multiple nodes,
        creating one or multible jobs.

        Keyword arguments:
        user        -- userid of the experimenter
        name        -- arbitrary identifier
        start       -- unix time stamp (UTC)
        duration    -- duration of the experiment in seconds
        nodecount   -- number of required nodes.
        nodetypes   -- filter on node type (static,spain|norway,-apu1)
        script      -- deployment URL to be fetched
        for the extra options, see README.md
        options     -- shared=1 (default 0)
                    -- recurrence, period, until
                    -- storage
                    -- traffic - bidi traffic per interface
                    -- nodes (list of node ids)
                    -- interfaces
                    -- restart
        """

        # TODO: any time this is called, check existing recurrent experiments
        #       for extension. This should be a quick check.
        # TODO: calculate and check total quota requirements before allocating

        try:
            start, duration = int(start), int(duration)
            nodecount = int(nodecount)
            assert nodecount > 0
        except Exception as ex:
            return None, "Start time and duration must be in integer seconds "\
                         "(unix timestamps), nodecount an integer > 0", {
                             "code": ERROR_PARSING_FAILED
                         }
        c = self.db().cursor()
        # confirm userid
        u = self.get_users(userid=user)
        if u is None:
            return None, "Unknown user.", {}
        u = u[0]
        ownerid = u['id']

        if u['quota_time'] < (duration * nodecount):
            return None, "Insufficient time quota.", \
                   {'quota_time': u['quota_time'],
                    'required': duration * nodecount}

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
        if u['quota_storage'] < (req_storage * nodecount):
            return None, "Insufficient storage quota.", \
                   {'quota_storage': u['quota_storage'],
                    'required': req_storage * nodecount}
        if u['quota_data'] < (req_traffic * nodecount * 3):
            return None, "Insufficient data quota.", \
                   {'quota_data': u['quota_data'],
                    'required': req_traffic * nodecount * 3}

        type_require, type_reject = self.parse_node_types(nodetypes)
        if type_require is None:
            error_message = type_reject
            return None, error_message, {}
        if DEPLOYMENT_RE.match(script) is None:
            if ['type:deployed'] in type_require:
                return None, "Deployed nodes can only schedule experiments " \
                             "hosted by %s" % DEPLOYMENT_SERVER, {}
            else:
                type_reject.append(['type:deployed'])

        if start == 0:
            start = self.get_scheduling_period()[0] + 10
        stop = start + duration

        try:
            intervals = self.get_recurrence_intervals(start, stop, opts)
            if len(intervals)<1:
                return None, "Something unexpected happened "\
                             "(no intervals generated)", {}
        except SchedulerException as ex:
            return None, ex.message, {}
        until = int(opts.get('until', 0))

        try:
            c.execute("INSERT INTO experiments "
                      "VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (name, ownerid, nodetypes, script, start, stop,
                       until, json.dumps(opts)))
            expid = c.lastrowid
            for inum, i in enumerate(intervals):
                nodes = self.get_available_nodes(
                            preselection, type_require, type_reject,
                            i[0], i[1])

                log.debug(
                    "Available nodes in interval (%s, %s): %s",
                    i[0],
                    i[1],
                    len(nodes))
                if len(nodes) < nodecount:
                    self.db().rollback()
                    msg = "Only %s/%s nodes are available during " \
                          "interval %s (%s,%s)." % \
                          (len(nodes), nodecount, inum + 1, i[0], i[1])
                    data = {"code": ERROR_INSUFFICIENT_RESOURCES,
                            "available": len(nodes),
                            "requested": nodecount,
                            "selection": preselection,
                            "start": i[0],
                            "stop": i[1]}
                    return None, msg, data

                nodes = nodes[:nodecount]
                for node in nodes:
                    c.execute("INSERT INTO schedule VALUES "
                              "(NULL, ?, ?, ?, ?, ?, ?, ?)",
                              (node['id'], expid, i[0], i[1], 'defined',
                               shared, json.dumps(deployment_opts)))

                c.execute("UPDATE node_interface SET "
                          "quota_value = quota_value - ? "
                          "WHERE nodeid = ?",
                          (req_traffic, node['id']))
                c.execute("UPDATE quota_owner_time SET current = ? "
                          "WHERE ownerid = ?",
                          (u['quota_time'] - (duration * nodecount), ownerid))
                c.execute("UPDATE quota_owner_storage SET current = ? "
                          "WHERE ownerid = ?",
                          (u['quota_storage'] - (req_storage * nodecount),
                           ownerid))
                c.execute("UPDATE quota_owner_data SET current = ? "
                          "WHERE ownerid = ?",
                          (u['quota_data'] - (req_traffic * nodecount * 3),
                           ownerid))
            self.db().commit()
            return expid, "Created experiment %s on %s nodes " \
                          "as %s intervals." % \
                          (expid, len(nodes), len(intervals)), {
                            "experiment": expid,
                            "nodecount": len(nodes),
                            "intervals": len(intervals)
                          }
        except db.Error as er:
            # NOTE: automatic rollback is triggered in case of an exception
            print er.message
            log.error(er.message)
            return None, "Task creation failed.", {'error': er.message}

    def delete_experiment(self, expid):
        c = self.db().cursor()
        c.execute("SELECT DISTINCT status FROM schedule WHERE expid = ?",
                  (expid,))
        result = c.fetchall()
        if len(result) == 0:
            return 0, "Could not find experiment id %s." % expid, {}
        statuses = set([x[0] for x in result])
        if set(['defined']) == statuses or set(['canceled']) == statuses:
            c.execute("DELETE FROM schedule WHERE expid = ?", (expid,))
            c.execute("DELETE FROM experiments WHERE id = ?", (expid,))
            self.db().commit()
            return 1, "Ok. Deleted experiment and scheduling entries", {}
        else:
            c.execute("""
UPDATE schedule SET status = ? WHERE
    status IN ('defined')
    AND expid = ?
                      """, ('canceled', expid))
            canceled = c.rowcount
            c.execute("""
UPDATE schedule SET status = ? WHERE
    status IN ('deployed', 'started', 'redeployed', 'restarted', 'running')
    AND expid = ?
                      """, ('aborted', expid))
            aborted = c.rowcount
            self.db().commit()
            return 1, "Ok. Canceled or aborted scheduling entries", {
                       "canceled": canceled,
                       "aborted": aborted
                   }

    def set_heartbeat(self, nodeid, seen):
        c = self.db().cursor()
        log.debug("Heartbeat on node %s seen %s" % (nodeid, seen))
        c.execute("UPDATE nodes SET heartbeat=? where id=?", (seen, nodeid))
        self.db().commit()

    def update_entry(self, schedid, status):
        c = self.db().cursor()
        c.execute("UPDATE schedule SET status=? where id=?", (status, schedid))
        self.db().commit()
        return c.rowcount
