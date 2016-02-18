#!/usr/bin/env python
# -*- encoding:utf-8 -*-

import xml.dom.minidom
import xmlrpclib
import simplejson as json

s = xmlrpclib.ServerProxy('http://localhost:9097')

if False:
  print "=== GetVersion ==="
  print json.dumps(s.GetVersion(), sort_keys=True, indent='  ')
  
  print "=== ListResources ==="
  resources = s.ListResources({}, {})
  print json.dumps(resources, sort_keys=True, indent='  ')
  
  print "=== Readable XML ==="
  print xml.dom.minidom.parseString(resources['value']).toprettyxml()

print "=== Allocate ==="
# slice_urn, credentials, rspecxml, options

reservation = """
<rspec type="request" xsi:schemaLocation="http://www.geni.net/resources/rspec/3 http://www.geni.net/resources/rspec/3/request.xsd " xmlns:client="http://www.protogeni.net/resources/rspec/ext/client/1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.geni.net/resources/rspec/3">
  <node client_id="node" component_manager_id="urn:publicid:IDN+foo+authority+cm" exclusive="true">
  </node>
  <services><install url="https://monroe1.cs.kau.se:5000/monroe/monroe_base"/></services>
</rspec>
"""

options = {
           "restart":"1",
           "start_time":"0",
           "stop_time":"10",
          }

result = s.Allocate('',{},reservation,options)
print json.dumps(result, sort_keys=True, indent='  ')
