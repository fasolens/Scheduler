# Marvin MONROE - a scheduling daemon

This package includes a relatively generic task scheduling daemon.

It is built to fulfill requirements of the European project MONROE, but
should be suitable in other types of distributed network with a central
controller.

## Installation

> python setup.py install

Copy and adjust the configuration files marvind.conf (on the node)
or marvinctld.conf (on the scheduling server).

## marvinctld

When started

  * synchronizes with an inventory REST API to retrieve node status.
  * opens the following control ports:
    * REST API
    * GENI v3 AM API

All state changes are stored in a SQLite database on disk, and will be
recovered on restart.

### REST endpoints

    routes = (
      '/version'          - protocol version number (GET)
      '/resources(|/.*)'   - node, type and status (GET, PUT)
      '/users(|/.*)'       - users (GET, POST, DELETE)
      '/experiments(|/.*)' - task definitions (GET, POST, DELETE)
      '/schedules(|/.*)'   - scheduled tasks (GET, PUT)
      '/backend(/.*)'     - backend actions (various)
    )

#### Terminology:

  * resource   (a node, with a node/resource id)
  * user       (a user, with a user id)
  * experiment (the definition of a task to be run on one or more nodes, with id)
  * schedules  (the n:m mapping of experiments to nodes, with id, start and stop time)

#### Access levels:
'#' is a placeholder for a node, task, user or scheduling id

for everyone:

  * GET version
  * GET backend/auth [verify SSL certificate]

for all authenticated clients:

  * GET resources
  * GET resources/#
  * GET resources/#/schedules
  * GET resources/#/experiments
  * GET resources/#/all
  * GET users
  * GET users/#
  * GET users/#/schedules
  * GET users/#/experiments
  * GET schedules?start=...&stop=...
  * GET schedules/#
  * GET schedules/find?nodecount=...&duration=...&start=...&nodetypes=...&nodes=...
  * GET experiments
  * GET experiments/#

only for users (role: user)

  * POST experiments      [+experiment and scheduling parameters]
  * DELETE experiments/#  [delete an experiment and its schedule entries]

only for administrators (role: admin)

  * PUT  resources        type=mobile|static|...
  * POST users            name=&ssl= [ssl fingerprint]
  * DELETE user/#

only for nodes (role: node)

  * GET resources/#/schedules
  * PUT resources/#       [send heartbeat]
  * PUT schedules/#        status=...

Valid status codes are:

  * defined    - experiment is created in the scheduler
  * deployed   - node has deployed the experiment, scheduled a start time
  * started    - node has successfully started the experiment
  * restarted  - node has restarted the experiment after a node failure

These status codes are final and cannot be overridden:

  * stopped    - experiment stopped by scheduler 
  * finished   - experiment completed, exited before being stopped
  * failed     - scheduling process failed
  * canceled   - user deleted experiment, task not deployed (but some were)
  * aborted    - user deleted experiment, task had been deployed

## Experiment and scheduling parameters:

The following parameters are used to schedule an experiment:

  * taskname  - an arbitrary identifier
  * start     - a UNIX time stamp, the start time (may be left 0 to indicate ASAP)
  * stop      - a UNIX time stamp, the stop time **OR**
  * duration  - runtime of the experiment in seconds.
  * nodecount - the number of nodes to allocate this experiment to
  * nodetypes - the type filter of required and rejected node types,
                Valid queries include a tag and value, e.g. type:testing
                Valid tags are [model, project, status, type]
                Supports the operators OR(|), NOT(-) and AND(,) in this strict order of precedence.
                EXAMPLE: type:testing,country:es,-model:apu2d
  * script    - the experiment to execute, currently in the form of a docker pull URL

These are defined as scheduling options, interpreted by the scheduling server:

* options   - (optional) additional scheduling options. 
    * nodes         - a specific list of node ids to select for scheduling
    * shared        - (default 0) 1 if this is a passive measurement experiment
    * recurrence    - (default 0) 'simple' for a basic recurrence model
    * period        - experiment will be repeated with this interval, in seconds
    * until         - UNIX timestamp, experiment will not be repeated after this date
    * restart     - (default 1) 0 if the experiment is not to be restarted if the node is rebooted
    * storage     - (default 1GB - container size?) storage quota for experiment results.
    * traffic     - traffic quota, per interface, bidirectional

Options that are required to be known during deployment are passed to the node as 
deployment parameters.

The options parameter should be x-www-form-urlencoded, that is separated by ampersands
and in the form key=value, or in the form of a JSON object.



#### Authentication:

This is based on client certificates. The server is supposed to be run behind a
HTTPS server (e.g. nginx) which will take care of verifying the certificate.
That server will then have to set the header HTTP_VERIFIED to NONE or SUCCESS
and the header HTTP_SSL_FINGERPRINT to the client certificate fingerprint

example nginx configuration:

    proxy_set_header  VERIFIED         $ssl_client_verify;
    proxy_set_header  SSL_FINGERPRINT  $ssl_client_fingerprint;

in order to use a web browser-based client, you also need to enable CORS headers, e.g.:

    add_header 'Access-Control-Allow-Origin' "$http_origin" always;
    add_header 'Access-Control-Allow-Credentials' 'true' always;
    add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS, PUT, DELETE' always;
    add_header 'Access-Control-Allow-Headers' 'DNT,X-CustomHeader,Keep-Alive,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type' always;

    if ($request_method = 'OPTIONS') {
        add_header 'Content-Length' 0 always;
        return 204;
    }

To create an authority, server or client keys, follow this guide:
http://dst.lbl.gov/~boverhof/openssl_certs.html

### Configuration

An example configuration is provided in ./files/etc/marvinctld.conf

The files follow the YAML (http://yaml.org) syntax. The required keys are subject to change, but all keys should be mentioned in the example files.

## marvind

When started

  * attempts to synchronize state with a marvinctld node controller.

For every new task assigned:

  * If it seen for the first time, downloads the task and runs a deployment
    hook. The start hook is then added to the systems atq (cron at queue) to
    be run at the scheduled time.
  * A stop hook is added to the atq at the scheduled stop time.

Task status is reported back to the ctld.

### Configuration

An example configuration is provided in ./files/etc/marvind.conf

# EXAMPLE setup

On the scheduling server:

  * Copy marvinctld.conf to /etc/marvinctld.conf and adjust address and port
    numbers. Make sure these are only available on the local machine.
  * Run marvinctld
  * Configure nginx to act as HTTPS proxy, and expose the HTTPS ports to
    the outside, e.g. measurement nodes.

On the measurement node:

  * Make sure the node is registered in the MONROE inventory (see packages
    metadata-exporter and autotunnel)
  * Generate and install client certificates
  * Copy marvind.conf to /etc/marvind.conf and adjust addresses, port numbers
    and certificate locations.
  * Run marvind

From anywhere:

  * Send REST commands to http://marvinctld-server:port/, e.g.

POST /user
Parameters: name, ssl, role

Creates user 'name' authenticated by the given ssl fingerprint and one
of the valid roles (user, node, admin)

POST /experiment
Parameters:

  * user      - user running the task
  * taskname  - arbitrary identifier
  * start     - unix timestamp, start time of the task
  * stop      - unix timestamp, stop time of the task
  * nodecount - number of nodes to assign this task to
  * nodetype  - type of nodes to assign this task to
  * script    - file to be downloaded, and made available to the deployment hook

# REST Return values:

  * 200 Ok          - Ok.
  * 201 Created     - Ok, a resource was created.
  * 400 Bad Request - Parameters are missing.
  * 404 Not Found   - The request path is unknown.
  * 409 Conflict    - Resource reservation failed + Reason.

### GENIv3 AM API

This API provides aggregate management according to the GENI v3 AM specifications.
It may have to be modified to conform to Fed4Fire specifications, but that will
require a working client to test against.

It provides the following methods:

  * GetVersion()
  * ListResources(credentials, options)
  * Allocate(slice_urn, credentials, rspecxml, options)

The following methods defined by the API are supported, but ignored:

  * Provision(urns, credentials, options)
  * PerformOperationalAction(urns, credentials, action, options)
  * Status(urns, credentials, options)
  * Describe(urns, credentials, options)
  * Renew(urns, credentials, stop_time, options)
  * Delete(urns, credentials, options)
  * Shutdown(urns, credentials, options)

# RSpec

In order to perform a reservation on MONROE nodes, the following RSpec parameters
and options are observed and required:

Required in RSpec:

  * type="request"
  * <services><install urn="...">

And either a list of

  * <node client_id="...">

Or a node count and type (TODO)

Required as Allocate ExtraOptions:

  * start_time
  * stop_time

# Example

    <rspec type="request" xsi:schemaLocation="http://www.geni.net/resources/rspec/3 http://www.geni.net/resources/rspec/3/request.xsd " xmlns:client="http://www.protogeni.net/resources/rspec/ext/client/1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.geni.net/resources/rspec/3">
      <services><install urn="http://..."/></services>
      <node client_id="PC" component_manager_id="urn:publicid:IDN+foo+authority+cm" exclusive="true"> </node>
    </rspec>

