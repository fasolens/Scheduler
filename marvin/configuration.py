#!/usr/bin/env python

import sys
import yaml

configuration = {}


def select(namespace, filename=None):
    if namespace not in configuration:
        if filename is None:
            sys.stderr.write(
                "ERROR:Configuration parser:Configuration %s not loaded.\n" %
                namespace)
            sys.exit(1)
        try:
            data = yaml.safe_load(open(filename, "r"))
            configuration[namespace] = data
        except Exception as ex:
            sys.stderr.write(
                "ERROR:Configuration parser:Could not parse %s: %s\n" %
                (filename, str(ex)))
            sys.exit(1)
    return configuration[namespace]
