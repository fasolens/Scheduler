#!/usr/bin/env python

import hmac
import hashlib
import requests
import simplejson as json
import logging
import time
import configuration

config = configuration.select('marvinctld')
logging.basicConfig(
    level=config['log']['level'],
    filename=config['log']['file'])
log = logging.getLogger('Inventory')


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
        "API_ID": config['inventory']['id'],
        "API_TIME": now,
        "API_HASH": hachee,
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
