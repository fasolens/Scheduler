#!/usr/bin/env python

"""
Marvinctld (MONROE scheduler).
Copyright (c) 2015 Celerway, Thomas Hirsch <thomas.hirsch@celerway.com>.
All rights reserved.

Manages a task schedule for MONROE nodes.
- Set up tasks using a GENI AM API v3 (tbd)
- Set up tasks via REST API (tbd)
- Synchronize schedule with marvind controlled nodes
"""

import sys
import logging
from logging.handlers import WatchedFileHandler
import configuration
import threading
import time

if len(sys.argv) < 2:
    cfile = "/etc/marvinctld.conf"
else:
    cfile = sys.argv[1]
config = configuration.select('marvinctld', cfile)

log = logging.getLogger('marvinctld')
log.addHandler(WatchedFileHandler(config['log']['file']))
log.setLevel(config['log']['level'])

from geniam import FederationAM
from restapi import RestAPI
from scheduler import Scheduler


def main():
    try:
        scheduler = Scheduler()
        am = FederationAM(scheduler)
        t2 = threading.Thread(target=am.start)
        rest = RestAPI(scheduler)
        t3 = threading.Thread(target=rest.start)
        t2.start()
        t3.start()
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        log.warning("Received interrupt in main")
        try:
            am.stop()
            rest.stop()
        except:
            pass
        try:
            t2.join()
            t3.join()
        except:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()
