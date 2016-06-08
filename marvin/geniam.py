#!/usr/bin/env python

"""
Marvinctld (MONROE scheduler) GENI Aggregate Manager API.
Copyright (c) 2015 Celerway, Thomas Hirsch <thomas.hirsch@celerway.com>.
All rights reserved.

Allows to schedule tasks through a GENI AMv3 API XML-RPC interface.
"""

import logging
from logging.handlers import WatchedFileHandler
import simplejson as json
import traceback
import configuration

from SimpleXMLRPCServer import SimpleXMLRPCServer as XRS
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler as XRH
from lxml import etree as e
from time import strftime
# from subprocess import check_output

config = configuration.select('marvinctld')

GENI_RT_SUCCESS = 0
GENI_RT_BADARGS = 1
GENI_RT_REFUSED = 7

RSPEC_NS = "http://www.geni.net/resources/rspec/3"
RSPEC = "{" + RSPEC_NS + "}%s"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
XSI = "{" + XSI_NS + "}%s"
XSI_SCHEMA_LOCATION = "http://www.geni.net/resources/rspec/3 " \
                      "http://www.geni.net/resources/rspec/3/ad.xsd"
NSMAP = {
  None: RSPEC_NS,
  "xsi": XSI_NS
}

log = logging.getLogger('GENI AM')
log.addHandler(WatchedFileHandler(config['log']['file']))
log.setLevel(config['log']['level'])


class Handler(XRH):

    """A dispatcher that allows to log error messages on server side"""

    def _dispatch(self, method, params):
        try:
            return self.server.funcs[method](*params)
        except:
            traceback.print_exc()
            raise


class FederationAM:

    def __init__(self, scheduler):
        self.scheduler = scheduler

    def return_code(self, value, code=GENI_RT_SUCCESS, output=""):
        return {
          "code": {
                "geni_code": code,
            },
          "value": value,
          "output": output
        }

    def GetVersion(self, options=None):
        log.debug("GetVersion %s" % json.dumps(options))
        return self.return_code({
            "geni_api": 3,
            "geni_api_versions": {
                "1": "http://"
                },
            "geni_am_type": ["monroeAM"],
            "geni_am_code_version": 0,
            "geni_credential_types": [{
                 "type": "geni_sfa",
                 "version": 3,
                }],
            "geni_request_rspec_versions": [{
                "type": "GENI",
                "version": "3",
                "schema":
                "http://www.geni.net/resources/rspec/3/request.xsd",
                "namespace":
                "http://www.geni.net/resources/rspec/3",
                "extensions":
                [
                  "http://hpn.east.isi.edu/rspec/ext/stitch/0.1/stitch-schema.xsd"]
                }],
            "geni_ad_rspec_versions": [{
                "type": "GENI",
                "version": "3",
                "schema":
                "http://www.geni.net/resources/rspec/3/ad.xsd",
                "namespace":
                "http://www.geni.net/resources/rspec/3",
                "extensions":
                [
                    "http://hpn.east.isi.edu/rspec/ext/stitch/0.1/stitch-schema.xsd"]
                }],
            })

    def RSpecAd(self, nodes):
        now = strftime("%Y-%m-%dT%H:%M:%SZ")
        root = e.Element(RSPEC % "rspec", type="advertisement",
                         generated=now,
                         valid_until=now,
                         # true and apparently acceptable
                         nsmap=NSMAP)
        root.attrib[XSI % "schemaLocation"] = XSI_SCHEMA_LOCATION
        for node in nodes:
            n = e.Element('node', component_name=node["hostname"],
                          component_uuid=str(node["id"])
                          )
            nt = e.Element('node_type', type_name="monroe_node")
            av = e.Element('available')
            ex = e.Element('exclusive')
            av.text = 'true'
            ex.text = 'true'
            n.append(nt)
            n.append(av)
            n.append(ex)
            root.append(n)

        return root

    def ListResources(self, credentials, options):
        log.debug(
            "ListResources %s %s" %
            (json.dumps(credentials), json.dumps(options)))
        # TODO check credentials
        # TODO check protocol version
        # TODO option :available   #filter available only
        # TODO option :compressed  #zlib
        # available = options.get('geni_available', False)
        # compressed = options.get('geni_compressed', False)

        nodes = self.scheduler.get_nodes()
        log.debug("Nodes: " + json.dumps(nodes))

        return self.return_code(e.tostring(self.RSpecAd(nodes),
                                xml_declaration=True, encoding="utf-8"))

    def get_allocate_params(self, rspec):
        if not rspec.get('type', '') == 'request':
            return None

        nodes = []
        for node in rspec.iterfind(RSPEC % "node"):
            nodes.append(node.get("client_id", None))
        script = None
        for services in rspec.iterfind(RSPEC % "services"):
            for install in services.iterfind(RSPEC % "install"):
                script = install.get('url', None)

        return {
          "nodes": nodes,
          "nodecount": "",
          "nodetype": "",
          "script": script
        }

    def Allocate(self, slice_urn, credentials, rspecxml, options):
        log.debug(
            "Allocate %s %s %s %s" % (json.dumps(credentials),
                                      json.dumps(options),
                                      json.dumps(slice_urn),
                                      json.dumps(rspecxml)))
        print rspecxml
        # NOTE: slice_urn is the experiment name
        start_time = options.get('start_time', None)
        stop_time = options.get('stop_time', None)
        spec = e.fromstring(rspecxml)
        params = self.get_allocate_params(spec)

        if params is None:
            return self.return_code("", GENI_RT_BADARGS,
                output="Wrong request type")
        if start_time is None or stop_time is None:
            return self.return_code("", GENI_RT_BADARGS,
                output="Missing parameter start_time/stop_time")
        nodecount = params.get('nodecount', None)
        nodeids = params.get('nodes', None)
        if nodecount is None and len(nodeids) == 0:
            return self.return_code("", GENI_RT_BADARGS,
                output="Missing parameter node_count or list of resources")
        script = params.get('script', None)
        if script is None:
            return self.return_code("", GENI_RT_BADARGS,
                output="Missing rspec for installing system image "
                       "via <services><install>")


        # TODO: check credentials and identify user id
        user = 1
        # TODO: identify RSpec for requesting nodes by type, not id
        nodetype = 'mobile'

        log.debug(
            "Requested to allocate %s %s %s %s %s %s %s %s" %
            (user, slice_urn, start_time, stop_time,
             nodecount, nodetype, script, options))

        taskid, message = self.scheduler.allocate(
            user, slice_urn, start_time, stop_time, nodecount, nodetype,
            script, json.dumps(options))

        if taskid is None:
            return self.return_code("", GENI_RT_REFUSED, output=message)
        else:
            return self.return_code("", GENI_RT_SUCCESS,
                output="Created new task with id %s." % taskid)

    def Provision(self, urns, credentials, options):
        return self.return_code("", GENI_RT_SUCCESS,
            output="Your allocated slivers are automatically provisioned "
                   "in a MONROE testbed.")

    def PerformOperationalAction(self, urns, credentials, action, options):
        return self.return_code("", GENI_RT_REFUSED,
            output="No operational actions are defined in this testbed.")

    def Status(self, urns, credentials, options):
        return self.return_code("", GENI_RT_REFUSED, output="Not implemented.")

    def Describe(self, urns, credentials, options):
        return self.return_code("", GENI_RT_REFUSED, output="Not implemented.")

    def Renew(self, urns, credentials, stop_time, options):
        return self.return_code("", GENI_RT_REFUSED, output="Not implemented.")

    def Delete(self, urns, credentials, options):
        return self.return_code("", GENI_RT_REFUSED, output="Not implemented.")

    def Shutdown(self, urns, credentials, options):
        return self.return_code("", GENI_RT_REFUSED, output="Not implemented.")

    def stop(self):
        self.server.shutdown()

    def start(self):
        address = (config['geni_api']['address'], config['geni_api']['port'])
        self.server = XRS(address, Handler)
        self.server.register_function(self.GetVersion)
        self.server.register_function(self.ListResources)
        self.server.register_function(self.Allocate)
        self.server.register_function(self.Provision)
        self.server.register_function(self.PerformOperationalAction)
        self.server.register_function(self.Status)
        self.server.register_function(self.Describe)
        self.server.register_function(self.Renew)
        self.server.register_function(self.Delete)
        self.server.register_function(self.Shutdown)
        self.server.serve_forever()
