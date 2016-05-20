#!/usr/bin/env python

"""
Marvinctld (MONROE scheduler) controlled node.
Copyright (c) 2015 Celerway, Thomas Hirsch <thomas.hirsch@celerway.com>.
All rights reserved.

Connects to a marvinctld and synchronizes scheduled tasks with the
local scheduling system (currently cron/atq).

usage: marvind.py configfile
"""

import sys
from glob import glob
from os import unlink
import logging
import configuration

import os
import errno
import zmq
import requests
import simplejson as json
import time
import traceback
import threading
from datetime import datetime
from subprocess import Popen, PIPE

requests.packages.urllib3.disable_warnings()

if len(sys.argv) < 2:
    cfile = "/etc/marvind.conf"
    print "usage: marvind.py [configuration]"
    print "Using default configuration at %s" % cfile
else:
    nope, cfile = sys.argv

config = configuration.select('marvind', cfile)
logging.basicConfig(
#logging.basicConfig(filename=config['log']['file'],
                    level=config['log']['level'])
log = logging.getLogger('marvind')

AT_TIME_FORMAT = "%H:%M %Y-%m-%d"

SYSEVENT_SCHEDULING_STARTED = "Scheduling.Started"

class SchedulingClient:
    running = threading.Event()
    jobs = {}
    status_queue = []
    # delayed status updates to be sent when we are online

    def stop(self):
        """soft interrupt signal, for use when threading"""
        self.running.clear()

    def __init__(self):
        id = config.get("id", None)
        if id is None:
            try:
                id = open(config.get("idfile", None), "r").read().strip()
                log.info("ID loaded from file: %s", id)
            except:
                pass
        if id is None:
            log.error("Node id not configured.")
        else:
            self.ID = id
            self.running.set()
        cert_file = config['ssl']['cert']
        key_file = config['ssl']['key']
        self.cert = (cert_file, key_file)

    def sysevent(self, eventType):
        try:
            socket = context.socket(zmq.REQ)
            socket.connect("ipc:///tmp/sysevent")
            socket.send("{\"EventType\": \"%s\"}" % (eventType,))
            socket.close(5)
        except Exception, ex:
            pass

    def resume_tasks(self):
        """When marvind is starting, it may be because the system has shut down.
        We should try to resume any containers that have failed or failed to
        start in the meantime.

        At this point we do not have an updated schedule (and may not be able
        to get one, because of connectivity issues), but we can resume any task
        that has configured a stop hook and is not finished.
        """
        jobs = self.read_jobs()
        relaunch = []
        for command in jobs.itervalues():
            if " " in command:
                hook, taskid = command.split(" ")
                if hook == self.stophook:
                    if self.starthook + " " + taskid in jobs.values():
                        continue
                    log.debug(
                        "During marvind startup, task %s had a stop hook, "
                        "but no start hook." % (taskid))
                    try:
                        if os.path.isfile("%s/%s.pid"):
                            log.debug("PID file still exists, restarting task %s" % taskid)
                            relaunch.append(taskid)
                    except Exception,ex:
                        log.debug("Could not check task PID, %s" % ex.message)
                        pass

        for taskid in relaunch:
            log.debug(
                "Restarting task %s: %s %s %s" %
                (taskid, self.starthook, taskid, "restarted"))
            pro = Popen(
                [self.starthook,
                 taskid,
                 "restarted"],
                stdout=PIPE,
                stdin=PIPE)
            pro.communicate()

    def add_task(self, task, sched):
        """upon querying a task, add it to local atq"""
        log.debug("add_task (%s, %s)" % (json.dumps(task), json.dumps(sched)))

        id   = str(sched['id'])

        starthook = self.starthook + " " + id
        stophook = self.stophook + " " + id

        timestamp = sched['start']
        deploy_conf = dict(sched['deployment_options'])
        deploy_conf.update({'script': task['script']})
        deploy_opts = json.dumps(deploy_conf)

        # run deploy hook, which should be safe to be re-run
        print [self.deployhook, id, task['script'], deploy_opts]
        fd = open("%s/%s.conf" % (self.confdir, id),'w')
        fd.write(deploy_opts)
        fd.close()
        log.debug(
            "Deploying task %s: %s " %
            (id, self.deployhook))
        pro = Popen(
            [self.deployhook, id],
            stdout=PIPE,
            stdin=PIPE)
        output, serr = pro.communicate()
        print output
        print serr
        if pro.returncode == 0:
            self.set_status(id, "deployed")
        else:
            # TODO detect acceptable failure codes (delayed deployment)
            return

        now  = int(time.time())
        if timestamp > now + 60:
            timestring = datetime.fromtimestamp(
                timestamp).strftime(
                    AT_TIME_FORMAT)  # we are losing the seconds
            log.debug("Trying to set at using %s" % timestring)
            pro = Popen(["at", timestring], stdout=PIPE, stdin=PIPE)
            output, serr = pro.communicate(input=starthook + "\n")
            print output
            print serr
            if pro.returncode != 0:
                log.warning(
                    "Atq start hook for task %s returned non-zero (%s). Failed." %
                    (id, pro.returncode))
                self.set_status(id, "failed")
                # TODO: handle tasks that failed scheduling
        else:
            log.warning(
                "Task %s has a past start time. Running %s" %
                (id, starthook))
            pro = Popen([self.starthook, id, "started"], stdout=PIPE, stdin=PIPE)
            output, serr = pro.communicate()
            print output
            print serr
            if pro.returncode != 0:
                log.warning(
                    "Start hook for task %s returned non-zero (%s). Failed." %
                    (id, pro.returncode))
                self.set_status(id, "failed")

        timestamp = sched['stop']
        timestring = datetime.fromtimestamp(
            timestamp).strftime(
                AT_TIME_FORMAT)  # we are losing the seconds
        log.debug("Trying to set at using %s" % timestring)
        pro = Popen(["at", timestring], stdout=PIPE, stdin=PIPE)
        output, serr = pro.communicate(input=stophook + "\n")
        print output
        print serr
        if pro.returncode != 0:
            log.error("Failed to set stop hook for task %i" % stophook)
            self.set_status(id, "failed")

        # TODO: handle tasks that failed scheduling
        # FIXME: if this happens, it is actually quite serious.
        # We should never keep a task alive that is not scheduled
        # to be terminated.

    def set_status(self, schedid, status):
        log.debug("Setting status for task %s to %s" % (schedid, status))
        deployed_msg = {
            "status": status,
            "schedid": schedid,
            "when": time.time(
            )}
        self.status_queue.append(deployed_msg)
        self.post_status()

    def post_status(self):
        try:
            for status in self.status_queue[:]:
                result = requests.put(
                    config['rest-server'] + '/schedules/' + status['schedid'],
                    data=status,
                    cert=self.cert,
                    verify=False)
                if result.status_code != 200:
                    log.debug("Setting status %s of task %s failed: %s" % \
                              (str(status), status['schedid'], result.text))
                else:
                    self.status_queue.pop()
        except Exception, ex:
            log.error("Exception in post_status: %s" % str(ex))
            pass

    def read_jobs(self):
        uname = config['marvind_username']

        pro = Popen(["atq"], stdout=PIPE)
        output = pro.communicate()[0].splitlines()
        atq = [line for line in output if line[-len(uname):] == uname]
        log.debug("atq:\n%s" % json.dumps(atq))

        jobs = {}
        for job in atq:
            atid = int(job.split("\t")[0])
            if atid not in jobs:
                pro = Popen(["at", "-c", str(atid)], stdout=PIPE)
                output = pro.communicate()[0]
                if pro.returncode == 1:
                    log.warning(
                        "atq has changed between calls to atq and at -c.")
                    continue
                else:
                    command = output.strip().splitlines()[-1]
                    jobs[atid] = command
                    log.debug("definition of task %s is %s" % (atid, command))

        return jobs

    def update_schedule(self, data):
        log.debug("update_schedule (%s)" % json.dumps(data))
        schedule = data[:3] # download the first three tasks only
        tasks = [x['id'] for x in schedule]

        # FIRST update scheduled tasks from atq
        jobs = self.read_jobs()
        known = jobs.values()

        for atid, command in jobs.iteritems():
            taskid = int(command.split(" ")[1]) if " " in command else ""
            if taskid not in tasks and "stop" not in command:
                #FIXME: actually run stop hook immediately if the task is deleted
                log.debug(
                    "deleting job %s from local atq, since %s not in %s (%s)" %
                    (atid, taskid, json.dumps(tasks), command))
                pro = Popen(["atrm", str(atid)], stdout=PIPE)
                pro.communicate()

        # SECOND fetch all remote tasks NOT in atq
        for sched in schedule:
            schedid = str(sched["id"])   # scheduling id. schedid n:1 taskid
            expid = str(sched["expid"])
            if sched["status"] in ['failed', 'finished', 'stopped', 'aborted', 'canceled']:
                log.debug(
                    "Not scheduling finished or aborted task "
                    "(Taskid %s, scheduling id %s)" % (expid, schedid))
                continue

            starthook = self.starthook + " " + schedid
            stophook = self.stophook + " " + schedid

            if starthook in known:
                log.debug("task %s is known and scheduled." % schedid)
                if not stophook in known:
                    # FIXME: we'll actually have to check if the wrapup task exists,
                    #       in case that the task was started already
                    # TODO: restore the stop hook - how can this happen?
                    pass
                continue
            elif stophook in known:
                log.debug("task %s is known and started." % schedid)
                try:
                    fd = open("%s/%s.pid" % (self.statdir, schedid))
                    pid = int(fd.read().strip())
                    fd.close()
                    try:
                        os.kill(pid, 0)
                    except OSError as err:
                        if err.errno == errno.ESRCH: # PID does no longer exist
                            self.set_status(schedid, 'finished')
                            unlink("%s/%s.pid" % (self.statdir, schedid))
                except:
                    log.debug("reading PID file for task %s failed" % schedid)
                continue
            else:
                log.debug("unknown task: %s" % schedid)
                result = requests.get(
                    config[
                        'rest-server'] +
                    '/experiments/' +
                    expid,
                    cert=self.cert,
                    verify=False)
                task = result.json()
                try:
                    self.add_task(task, sched)
                except IndexError:
                    traceback.print_exc(file=sys.stdout)
                    log.error(
                        "Fetching experiment %s did not return a task "
                        "definition, but %s" % (expid, task))

        # FINALLY read and post task status from *.status
        try:
            statfiles = glob(self.statdir + "/*.status")
            for f in statfiles:
                schedid = f.split("/")[-1].split(".status")[0]
                with open(f) as fd:
                    status = fd.read().strip()
                    self.set_status(schedid, status)
                    fd.close()
                unlink(f)
        except Exception,ex:
	    log.error("Error reading or sending experiment status. %s" % str(ex))



    def start(self):
        self.starthook = config['hooks']['start']
        self.stophook = config['hooks']['stop']
        self.deployhook = config['hooks']['deploy']
        self.statdir = config['status_directory']
        self.confdir = config['config_directory']

        result = requests.get(
            config['rest-server'] + "/backend/auth",
            data=None,
            cert=self.cert,
            verify=False)
        log.debug("Authenticated as %s" % result.text)
        if result.status_code == 401:
            log.error("Node certificate not valid.")
            return

        self.sysevent(SYSEVENT_SCHEDULING_STARTED)

        self.resume_tasks()

        while self.running.is_set():
            try:
                while self.running.is_set():
                    heartbeat = config[
                        'rest-server'] + "/resources/" + str(self.ID)
                    result = requests.put(
                        heartbeat,
                        data=None,
                        cert=self.cert,
                        verify=False)
                    if result.status_code == 200:
                        self.update_schedule(result.json())
                        self.post_status()
                    else:
                        log.debug(
                            "Scheduling server is not available. (Status %s)" %
                            result.status_code)
                    time.sleep(config['heartbeat_period'])

            except (KeyboardInterrupt, SystemExit):
                traceback.print_exc(file=sys.stdout)
                log.warning("Received INTERRUPT.")
                break
            except IOError as e:
                traceback.print_exc(file=sys.stdout)
                log.error("IOError %s" % e.message)
                time.sleep(60) # trying again after 60 seconds
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
                log.error(
                    "Failed connection to %s. Trying again in 5s." %
                    config['rest-server'])
                log.debug(e.message)
                time.sleep(5)


def main():
    SchedulingClient().start()
    sys.exit(0)

if __name__ == "__main__":
    main()
