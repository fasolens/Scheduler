#!/usr/bin/env python

"""
Marvinctld (MONROE scheduler) REST API.
Copyright (c) 2015 Celerway, Thomas Hirsch <thomas.hirsch@celerway.com>.
All rights reserved.

Allows to schedule tasks through a REST API interface.
"""

import logging
import configuration

import simplejson as json
import time
import web
import scheduler

config = configuration.select('marvinctld')

logging.basicConfig(
    filename=config['log']['file'],
    level=config['log']['level'])
log = logging.getLogger('REST API')

API_VERSION = "1.0"
# NOTE: major versions will be reflected in the URL
#       minor versions will increase after first deployment, and should not
#       break compatibility with prior minor versions.


def dumps(data):
    return json.dumps(data, sort_keys=True, indent='  ')


def set_headers(web):
    web.header('Content-type', 'application/json')
    web.header('API-Version', API_VERSION)


class ApiServer(web.application):

    def run(self, port=config['rest_api']['port'], *middleware):
        fn = self.wsgifunc(*middleware)
        return web.httpserver.runsimple(fn, (config['rest_api']['address'],
                                             config['rest_api']['port']))


class Version:

    def GET(self):
        set_headers(web)
        return dumps(API_VERSION)


# RESOURCE ##################################################################
class Resource:

    def GET(self, nodeid):
        role = rest_api.get_role(web.ctx)
        if role is None:
            web.ctx.status = '401 Unauthorized'
            return "Please present a valid SSL client certificate to access this information"

        data = None
        if nodeid in ["", "/"]:
            data = rest_api.scheduler.get_nodes()
        else:
            path = nodeid.split("/")
            if len(path) > 2 and path[2] == 'schedule':
                data = rest_api.scheduler.get_schedule(nodeid=path[1])
            elif len(path) > 2 and path[2] == 'all':
                data = {
                  'schedule': rest_api.scheduler.get_schedule(nodeid=path[1]),
                  'tasks': rest_api.scheduler.get_experiments(nodeid=path[1])
                }
            else:
                data = rest_api.scheduler.get_nodes(nodeid=path[1])

        if data is None:
            web.ctx.status = '404 Not Found'
            return "Could not find resource with this id."

        set_headers(web)
        return dumps(data)

    def PUT(self, nodeid):
        if nodeid in ["", "/"]:
            web.ctx.status = '404 Not Found'
            return "Updating collection not allowed."
        nodeid = nodeid[1:]
        data = web.input()

        role = rest_api.get_role(web.ctx)
        if "type" in data.keys():
            if role == scheduler.ROLE_ADMIN:
                result = rest_api.scheduler.set_node_types(nodeid,
                                                           data['type'])
                if result is True:
                    return "Node type set."
                else:
                    web.ctx.status = '404 Not Found'
                    return result
            else:
                web.ctx.status = '401 Unauthorized'
                return "You'd have to be an admin to do that"
        elif role == scheduler.ROLE_NODE:
            now = int(time.time())
            rest_api.scheduler.set_heartbeat(nodeid, now)
            data = rest_api.scheduler.get_schedule(nodeid=nodeid)
            return dumps(data)
        else:
            web.ctx.status = '400 Bad Request'
            return "Parameters missing: type\nIf you are a node, " \
                   "you were identified as SSL_ID %s." % \
                   web.ctx.env.get('HTTP_SSL_FINGERPRINT', None)

# SCHEDULE ##################################################################


class Schedule:  # allocate

    def GET(self, resource):
        role = rest_api.get_role(web.ctx)
        if role is None:
            web.ctx.status = '401 Unauthorized'
            return "Please present a valid SSL client certificate to access this information"

        params = web.input()
        if resource in ["", "/"]:
            tasks = rest_api.scheduler.get_schedule(
                        start = params.get('start',0),
                        stop = params.get('stop',0)
                    )
        elif resource == "/find":
            tasks = rest_api.scheduler.find_slot(
                        params.get('nodecount', 1),
                        params.get('duration', 1),
                        params.get('start', 0),
                        params.get('nodetypes', ''),
                        params.get('results', 1),
                    )
        else:
            schedid = resource[1:]
            tasks = rest_api.scheduler.get_schedule(schedid=schedid)

        if tasks is None:
            web.ctx.status = '404 Not Found'
            return "Could not find schedule entry with this id."

        set_headers(web)
        return dumps(tasks)

    def PUT(self, schedid):
        role = rest_api.get_role(web.ctx)
        if role != scheduler.ROLE_NODE:
            web.ctx.status = '401 Unauthorized'
            return "You'd have to be a node to do that"
        params = web.input()
        if 'status' not in params:
            web.ctx.status = '400 Bad Request'
            return "Parameters missing (required: status)"
        elif schedid in ["", "/"]:
            web.ctx.status = '400 Bad Request'
            return "Scheduling id missing."
        schedid = schedid[1:]
        if params['status'] in scheduler.TASK_STATUS_CODES:
            rest_api.scheduler.set_status(
                schedid=schedid,
                status=params['status'])
            return "Ok."
        else:
            web.ctx.status = '400 Bad Request'
            return "Unknown status code."

# EXPERIMENT ################################################################


class Experiment:

    def GET(self, task):
        role = rest_api.get_role(web.ctx)
        if role is None:
            web.ctx.status = '401 Unauthorized'
            return "Please present a valid SSL client certificate to access this information"

        if task in ["", "/"]:
            tasks = rest_api.scheduler.get_experiments()
        else:
            expid = task[1:]
            tasks = rest_api.scheduler.get_experiments(expid=expid)

        if tasks is None:
            web.ctx.status = '404 Not Found'
            return "Could not find experiment with this id."

        set_headers(web)
        return dumps(tasks)

    def POST(self, ignored):
        user, role = rest_api.get_user(web.ctx)
        if role != scheduler.ROLE_USER:
            web.ctx.status = '401 Unauthorized'
            return "You'd have to be a user to do that."

        params = {}
        try:
            params = json.loads(web.data())
        except:
            params = web.input()
        required = ['name', 'start', 'stop', 'nodecount', 'nodetypes', 'script']
        optional = ['options']
        if set(required).issubset(set(params.keys())):
            alloc, error = rest_api.scheduler.allocate(
                              user, params['name'],
                              params['start'], params['stop'],
                              params['nodecount'], params['nodetypes'],
                              params['script'], params.get('options', ''))
            if alloc is not None:
                web.header('Location', "/schedule/%i" % alloc)
                web.ctx.status = '201 Created'
                return "Allocated task %s." % alloc
            else:
                web.ctx.status = '409 Conflict'
                return "Could not allocate. %s" % error
        else:
            web.ctx.status = '400 Bad Request'
            return "Parameters missing " \
                   "(required: %s | optional: %s, provided: %s)." \
                   % (str(required), str(optional), str(params.keys()))

    def DELETE(self, expid):
        role = rest_api.get_role(web.ctx)
        if role not in [scheduler.ROLE_USER, scheduler.ROLE_ADMIN]:
            web.ctx.status = '401 Unauthorized'
            return "You'd have to be a user or admin to do that"

        if expid in ["", "/"]:
            web.ctx.status = '400 Bad Request'
            return "Taskid missing."
        else:
            result = rest_api.scheduler.delete_experiment(expid[1:])
            log.debug("Delete result: %s rows deleted" % result)
            if result > 0:
                return "Ok. Deleted task and scheduling entries."
            else:
                web.ctx.status = '404 Not Found'
                return "Could not find task id."

# USER ######################################################################


class User:

    def GET(self, userid):
        role = rest_api.get_role(web.ctx)
        if role is None:
            web.ctx.status = '401 Unauthorized'
            return "Please present a valid SSL client certificate to access this information"

        data = None
        log.debug(userid)
        if userid in ["", "/"]:
            data = rest_api.scheduler.get_users()
        else:
            path = userid.split("/")
            if len(path) > 2 and path[2] == 'schedule':
                data = rest_api.scheduler.get_schedule(userid=path[1])
            elif len(path) > 2 and path[2] == 'experiments':
                data = rest_api.scheduler.get_experiments(userid=path[1])
            else:
                data = rest_api.scheduler.get_users(path[1])

        if data is None:
            web.ctx.status = '404 Not Found'
            return "Could not find user with this id."

        set_headers(web)
        return dumps(data)

    def POST(self, ignored):
        role = rest_api.get_role(web.ctx)
        if role != scheduler.ROLE_ADMIN:
            web.ctx.status = '401 Unauthorized'
            return "You'd have to be an admin to do that (%s, %s)" % \
                   (role, scheduler.ROLE_ADMIN)

        data = web.input()
        if "name" in data and "ssl" in data and "role" in data:
            userid, error = rest_api.scheduler.create_user(
                data['name'], data['ssl'], data['role'])
            if userid is not None:
                web.ctx.status = '201 Created'
                web.header('Location', "/user/%i" % userid)
                return "User %s created." % userid
            else:
                web.ctx.status = '409 Conflict'
                return error
        else:
            web.ctx.status = '400 Bad Request'
            return "Parameters missing (name, ssl, role)."

    def DELETE(self, userid):
        role = rest_api.get_role(web.ctx)
        if role != scheduler.ROLE_ADMIN:
            web.ctx.status = '401 Unauthorized'
            return "You'd have to be an admin to do that"

        if userid in ["", "/"]:
            web.ctx.status = '400 Bad Request'
            return "Userid missing."
        else:
            result = rest_api.scheduler.delete_user(userid[1:])
            log.debug("Delete result: %s" % result)
            if result is True:
                return "Ok. Deleted user, tasks and scheduling entries."
            else:
                web.ctx.status = '404 Not Found'
                return "Could not find user with that id."

# BACKEND ###################################################################


class Backend:

    def GET(self, action):
        if action == "/auth":
            verified = web.ctx.env.get('HTTP_VERIFIED', None)
            fingerprint = web.ctx.env.get('HTTP_SSL_FINGERPRINT', '')
            user = rest_api.scheduler.get_users(ssl=fingerprint)

            if user is None or user[0]["role"] == scheduler.ROLE_INVALID:
                web.ctx.status = '401 Unauthorized'
            if user is not None:
                user = user[0]

            return dumps({
                         "verified": verified,
                         "fingerprint": fingerprint,
                         "user": user
                         })

        else:
            web.ctx.status = '404 Not Found'
            return "Unknown request"

    def PUT(self, action):
        role = rest_api.get_role(web.ctx)
        if role != scheduler.ROLE_ADMIN:
            web.ctx.status = '401 Unauthorized'
            return "You'd have to be an admin to do that"
        pass


routes = (
  '/version', 'Version',
  '/v1/resources(|/.*)', 'Resource',
  '/v1/users(|/.*)', 'User',
  '/v1/experiments(|/.*)', 'Experiment',
  '/v1/schedule(|/.*)', 'Schedule',
  '/v1/backend(/.*)', 'Backend',
)


class RestAPI:

    def __init__(self, scheduler):
        global rest_api
        self.scheduler = scheduler
        rest_api = self

    def stop(self):
        log.debug("Web server might take a second to shut down.")
        self.app.stop()

    def start(self):
        web.config.debug = True
        self.app = ApiServer(routes, globals())
        self.app.run()

    def get_fingerprint(self, ctx):
        fingerprint = ctx.env.get('HTTP_SSL_FINGERPRINT', None)
        return fingerprint

    def get_role(self, ctx):
        role = self.scheduler.get_role(self.get_fingerprint(web.ctx))
        return role

    def get_user(self, ctx):
        user = self.scheduler.get_users(ssl=self.get_fingerprint(web.ctx))
        if user is None or len(user) == 0:
            return None
        return user[0]['id'], user[0]['role']  # user, role
