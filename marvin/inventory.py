#!/usr/bin/env python

import hmac
import hashlib
import requests
import simplejson as json
import logging
from logging.handlers import WatchedFileHandler
import time
import configuration

config = configuration.select('marvinctld')
log = logging.getLogger('Inventory')
log.addHandler(WatchedFileHandler(config['log']['file']))
log.setLevel(config['log']['level'])


def inventory_api(route, data=None):
    now = str(int(time.time()))
    sdata = "" if data is None else json.dumps(data)
    message = now + str(config['inventory']['id']) + sdata
    hachee = hmac.new(
          key=config['inventory']['key'],
          msg=message,
          digestmod=hashlib.sha256
        ).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "Client-Id": config['inventory']['id'],
        "Message-Timestamp": now,
        "Message-Hash": hachee,
    }
    log.debug("API request %s with params %s" % (route, json.dumps(headers)))
    log.debug("Hashed message: %s" % message)
    r = requests.get(config['inventory']['url'] + route, headers=headers)
    log.debug("Reply: %s" % r.text)
    if "Access denied" not in r.text:
        try:
            return json.loads(r.text)
        except:
            return None
    else:
        log.error("Could not authenticate with inventory.")
        return None
