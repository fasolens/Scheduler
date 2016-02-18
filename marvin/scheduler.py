#!/usr/bin/env python

import sys
import sqlite3 as db
import time
import logging
from thread import get_ident
import threading
from inventory import inventory_api
import configuration
import simplejson as json

config = configuration.select('marvinctld')

logging.basicConfig(
    filename=config['log']['file'],
    level=config['log']['level'])
log = logging.getLogger('Scheduler')

log.debug("Configuration loaded: " + str(config))

TASK_STATUS_CODES = [
    'defined', 'deployed', 'started', 'redeployed', 'restarted',
    'finished',
    'failed',
]

# POLICY CHECKS AND VALUES
# task may be scheduled # seconds after NOW
POLICY_TASK_DELAY = 60
# task must be scheduled for a minimum of # seconds
POLICY_TASK_MIN_RUNTIME = 5 * 60
# recurrence may only happen with a minimum period of # seconds
POLICY_TASK_MIN_RECURRENCE = 3600
# scheduling may only happen # seconds in advance
POLICY_SCHEDULING_PERIOD = 31 * 24 * 3600

NODE_MISSING = 'missing'  # existed in the past, but no longer listed in the inventory
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


class SchedulerException(Exception):
    pass


class Scheduler:

    def __init__(self):
        self.check_db()
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
            status = NODE_ACTIVE if node[
                "Status"] == u'DEPLOYED' or node[
                    "Status"] == u'TESTING' else NODE_DISABLED
            c.execute(
                "UPDATE nodes SET hostname = ?, status = ? WHERE id = ?",
                (node["HostName"],
                 status,
                 node["NodeId"]))
            c.execute(
                "INSERT OR IGNORE INTO nodes VALUES (?, ?, ?, ?)",
                (node["NodeId"],
                 node["HostName"],
                    status,
                    0))
            c.execute(
                "INSERT OR IGNORE INTO node_type VALUES (?, ?)",
                (node["NodeId"],
                 NODE_TYPE_STATIC))
        self.db().commit()

    connections = {}

    def db(self):
        id = get_ident() or threading.current_thread().ident
        if not self.connections.get(id):
            log.debug("Connection opened for thread id %s" % id)
            self.connections[id] = db.connect(config['database'])
            self.connections[id].row_factory = db.Row
        return self.connections[id]

    def check_db(self):
        c = self.db().cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = c.fetchall()
        # TODO: more sanity checks on boot
        if not set(["nodes", "node_type", "owners",
                    "experiments", "schedule"]).issubset(set(tables)):
            for statement in """

CREATE TABLE IF NOT EXISTS nodes (id INTEGER PRIMARY KEY ASC,
    hostname TEXT NOT NULL, status TEXT, heartbeat INTEGER);
CREATE TABLE IF NOT EXISTS node_type (nodeid INTEGER NOT NULL,
    type TEXT NOT NULL, FOREIGN KEY (nodeid) REFERENCES nodes(id),
    PRIMARY KEY (nodeid, type));
CREATE TABLE IF NOT EXISTS owners (id INTEGER PRIMARY KEY ASC,
    name TEXT UNIQUE NOT NULL, ssl_id TEXT UNIQUE NOT NULL,
    role TEXT NOT NULL);
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
            c.execute("SELECT * FROM nodes WHERE EXISTS "
                      "(SELECT nodeid FROM node_type WHERE type = ?)",
                      (nodetype,))
        else:
            c.execute("SELECT * FROM nodes")
        noderows = c.fetchall()
        nodes = [dict(x) for x in noderows] or None
        if nodes:
            for node in nodes:
                c.execute(
                    "SELECT type FROM node_type WHERE nodeid = ?", (node['id'],))
                typerows = c.fetchall()
                node['type'] = [row['type'] for row in typerows]
        return nodes

    def set_node_types(self, nodeid, nodetypes):
        c = self.db().cursor()
        types = set(nodetypes.split(","))
        try:
            c.execute("DELETE FROM node_type WHERE nodeid = ?", (nodeid,))
            for type_ in types:
                c.execute(
                    "INSERT INTO node_type VALUES (?, ?)",
                    (nodeid,
                     type_.strip()))
            self.db().commit()
            if c.rowcount == 1:
                return True
            else:
                return "Node id not found."
        except db.Error as er:
            log.warning(er.message)
            return "Error updating node."

    def get_users(self, userid=None, ssl=None):
        c = self.db().cursor()
        if userid is not None:
            c.execute("SELECT * FROM owners where id = ?", (userid,))
        elif ssl is not None:
            c.execute("SELECT * FROM owners where ssl_id = ?", (ssl,))
        else:
            c.execute("SELECT * FROM owners")
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

    def create_user(self, name, ssl, role):
        c = self.db().cursor()
        try:
            c.execute(
                "INSERT INTO owners VALUES (NULL, ?, ?, ?)", (name, ssl, role))
            self.db().commit()
            return c.lastrowid, None
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
                     userid=None, past=False, start=0, stop=0):
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
        pastquery = (" AND NOT (start>%i OR stop<%i)" % (stop, start)
                    ) if not past else ""
        if schedid is not None:
            c.execute(
                "SELECT * FROM schedule WHERE id = ?" + pastquery, (schedid,))
        elif expid is not None:
            c.execute(
                "SELECT * FROM schedule WHERE expid = ?" + pastquery, (expid,))
        elif nodeid is not None:
            c.execute(
                "SELECT * FROM schedule WHERE nodeid=?" + pastquery, (nodeid,))
        elif userid is not None:
            c.execute("SELECT * FROM schedule s, experiments t "
                      "WHERE s.expid = t.id AND ownerid=?" +
                      pastquery, (userid,))
        else:
            c.execute("SELECT * FROM schedule WHERE 1=1" + pastquery)
        taskrows = c.fetchall()
        tasks = [dict(x) for x in taskrows]
        for x in tasks:
            x['deployment_options'] = json.loads(
                x.get('deployment_options', '{}'))
        return tasks

    def set_status(self, schedid, status):
        c = self.db().cursor()
        if status in TASK_STATUS_CODES:
            c.execute(
                "UPDATE schedule SET status = ? WHERE id = ?",
                (status, schedid))
            self.db().commit()
            if c.rowcount == 1:
                return True
        return False

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
                "SELECT id FROM schedule WHERE id=?",
                (experiments[i]['id'],
                 ))
            schedids = [dict(x)['id'] for x in c.fetchall()]
            experiments[i]['schedids'] = schedids
            experiments[i]['options'] = json.loads(task.get('options', '{}'))
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

        until = max(self.get_scheduling_period()[1], int(until))

        if start < now + POLICY_TASK_DELAY:
            raise SchedulerException(
                "Tasks may not be scheduled immediately or in the past "
                "(%i second delay)." % POLICY_TASK_DELAY)
        if stop < start + POLICY_TASK_MIN_RUNTIME:
            raise SchedulerException(
                "Tasks must run for a minimum of %i seconds." %
                POLICY_TASK_MIN_RUNTIME)

        if recurrence is None:
            return [(start, stop)]
        elif recurrence == "simple":
            delta = stop - start
            if period < delta:
                raise SchedulerException("Recurrence period too small. "
                                         "Must be greater than task runtime.")
            if period < POLICY_TASK_MIN_RECURRENCE:
                raise SchedulerException("Recurrence period too small. "
                                         "Must be greater than %i seconds." %
                                         POLICY_TASK_MIN_RECURRENCE)
            intervals = [(fro, fro + delta)
                         for fro in xrange(start, until, period)]
            return intervals
        else:
            return []

    def get_available_nodes(self, type_require, type_reject, start, stop):
        """ Select all active nodes not having a task scheduled between
            start and stopfrom the set of nodes matching type_accept and
            not type_reject
        """

        c = self.db().cursor()

        query = "\nSELECT DISTINCT id FROM nodes WHERE status = ? \n"
        for type_ in type_require:
            query += "  AND EXISTS (SELECT nodeid FROM node_type " \
                     "WHERE type = ?) \n"
        for type_ in type_reject:
            query += "  AND NOT EXISTS (SELECT nodeid FROM node_type "\
                     "WHERE type = ?) \n"
        query += """
AND id NOT IN (
    SELECT DISTINCT nodeid FROM schedule s
    WHERE shared = 0 AND NOT ((s.stop < ?) OR (s.start > ?))
)
                 """
        c.execute(query, [NODE_ACTIVE] + type_require +
                  type_reject + [start, stop])

        noderows = c.fetchall()
        nodes = [dict(x) for x in noderows]
        return nodes

    def parse_node_types(self, nodetypes=""):
        try:
            types = nodetypes.split(",")
            type_reject = [t[1:].strip() for t in types
                    if len(t)>2 and t[0] == '-']
            type_require = [t.strip() for t in types
                    if len(t)>1 and t[0] != '-']
            return type_require, type_reject
        except Exception,ex:
            return None, "nodetype expression could not be parsed. "+ex.message

    def find_slot(self, nodecount=1, duration=1, start=0, 
                  nodetypes="", results=1):
        """find the next available slot given certain criteria"""

        print "find_slot %s %s %s %s" % (nodecount, duration, start, nodetypes)
        start, duration, nodecount = int(start), int(duration), int(nodecount)
        period     = self.get_scheduling_period()
        start = max(start, period[0])
        stop  = period[1]

        type_require, type_reject = self.parse_node_types(nodetypes)
        if type_require is None:
            error_message = type_reject
            return "None", error_message

        # fetch all schedule segmentations (experiments starting or stopping)
        c = self.db().cursor()
        query = """
SELECT DISTINCT * FROM (
    SELECT start AS t FROM experiments UNION
    SELECT stop  AS t FROM experiments
) WHERE t >= ? AND t < ? ORDER BY t ASC;
                """
        c.execute(query, (start, stop))
        segments = [start] + [x[0] for x in c.fetchall()] + [stop]

        slots = []

        while len(segments)>1:
            s0 = segments[0]
            c = 1

            while segments[c]-s0 < duration:
                if c==len(segments)-1:
                    return []
                c+=1

            nodes = self.get_available_nodes(
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
                if len(slots)>=results:
                  return slots
            # TODO: ideally, we identify the segment where the conflict occurs
            # and skip until after this segment. until then, we just iterate.

            segments.pop(0)

        if len(slots)>0:
            return slots
        else:
            return None, "Could not find available time slot "\
                         "matching these criteria."


    def allocate(self, user, name, start, stop, nodecount,
                 nodetypes, script, options):
        """Insert a new task on one or multiple nodes,
        creating one or multible jobs.

        Keyword arguments:
        user        -- userid of the experimenter
        name        -- arbitrary identifier
        start, stop -- unix time stamp (UTC)
        nodecount   -- number of required nodes.
        nodetypes   -- filter on node type (static,spain,-apu1)
        script      -- deployment URL to be fetched
        for the extra options, see README.md
        options     -- shared=1 (default 0)
                    -- recurrence, period, until
                    -- traffic_in, traffic_out, storage
                    -- interfaces
                    -- restart
        """

        # TODO: any time this is called, check existing recurrent experiments
        #       for extension. This should be a quick check.

        start, stop = int(start), int(stop)
        c = self.db().cursor()
        # confirm userid
        c.execute("SELECT id FROM owners WHERE id = ?", (user,))
        owner = c.fetchone()
        if owner is None:
            return None, "Unknown user."
        ownerid = owner['id']

        try:
            opts = json.loads(options)
        except:
            try:
                opts = dict([opt.split("=")
                             for opt in options.split("&")]) if options else {}
            except Exception as ex:
                return None, "options string could not be parsed. "+ex.message

        shared = 1 if opts.get('shared', 0) else 0

        deployment_keys = [
            'traffic_in',
            'traffic_out',
            'storage',
            'interfaces',
            'restart']
        scheduling_keys = ['shared', 'recurrence', 'period', 'until']
        deployment_opts = dict([(key, opts.get(key, None))
                               for key in deployment_keys if key in opts])
        opts = dict([(key, opts.get(key, None))
                    for key in scheduling_keys if key in opts])

        type_require, type_reject = self.parse_node_types(nodetypes)
        if type_require is None:
            error_message = type_reject
            return None, error_message

        try:
            intervals = self.get_recurrence_intervals(start, stop, opts)
        except SchedulerException as ex:
            return None, ex.message
        until = int(opts.get('until', 0))


        try:
            c.execute("INSERT INTO experiments "
                      "VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (name, ownerid, nodetypes, script, start, stop,
                       until, json.dumps(opts)))
            expid = c.lastrowid
            for inum, i in enumerate(intervals):
                nodes = self.get_available_nodes(
                           type_require, type_reject, i[0], i[1])


                nodecount = int(nodecount)
                log.debug(
                    "Available nodes in interval (%s, %s): %s",
                    i[0],
                    i[1],
                    len(nodes))
                if len(nodes) < nodecount:
                    self.db().rollback()
                    return None, "Only %s/%s nodes are available during " \
                                 "interval %s (%s,%s)." % \
                                 (len(nodes), nodecount, inum + 1, i[0], i[1])

                nodes = nodes[:nodecount]
                for node in nodes:
                    c.execute("INSERT INTO schedule VALUES "
                              "(NULL, ?, ?, ?, ?, ?, ?, ?)",
                              (node['id'], expid, start, stop, 'defined',
                               shared, json.dumps(deployment_opts)))

            self.db().commit()
            return expid, "Created experiment %s on %s nodes " \
                          "as %s intervals." % \
                          (expid, len(nodes), len(intervals))
        except db.Error as er:
            # NOTE: automatic rollback is triggered in case of an exception
            log.error(er.message)
            return None, "Task creation failed."

    def delete_experiment(self, expid):
        c = self.db().cursor()
        c.execute("DELETE FROM schedule WHERE id = ?", (expid,))
        c.execute("DELETE FROM experiments WHERE id = ?", (expid,))
        self.db().commit()
        return c.rowcount

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
