#!/bin/bash
set -e

SCHEDID=$1
STATUS=$2
CONTAINER=monroe-$SCHEDID

BASEDIR=/experiments/user
STATUSDIR=$BASEDIR
mkdir -p $BASEDIR

if [ -f $BASEDIR/$SCHEDID.conf ]; then
  CONFIG=$(cat $BASEDIR/$SCHEDID.conf);
  IS_INTERNAL=$(echo $CONFIG | jq -r '.internal // empty');
  IS_SSH=$(echo $CONFIG | jq -r '.ssh // empty');
  BDEXT=$(echo $CONFIG | jq -r '.basedir // empty');
fi
if [ ! -z "$IS_INTERNAL" ]; then
  BASEDIR=/experiments/monroe${BDEXT}
  mkdir -p $BASEDIR
  echo $CONFIG > $BASEDIR/$SCHEDID.conf
  # redirect output to log file
  exec > $BASEDIR/start.log 2>&1
else
  exec >> $BASEDIR/$SCHEDID/start.log 2>&1
fi

NOERROR_CONTAINER_IS_RUNNING=0

ERROR_CONTAINER_DID_NOT_START=10
ERROR_NETWORK_CONTEXT_NOT_FOUND=11
ERROR_IMAGE_NOT_FOUND=12
ERROR_MAINTENANCE_MODE=13

echo -n "Checking for maintenance mode... "
MAINTENANCE=$(cat /monroe/maintenance/enabled || echo 0)
if [ $MAINTENANCE -eq 1 ]; then
   echo 'failed; node is in maintenance mode.' > $STATUSDIR/$SCHEDID.status
   echo "enabled."
   exit $ERROR_MAINTENANCE_MODE;
fi
echo "disabled."

echo -n "Ensure network and containers are set up... "
mkdir -p /var/run/netns

# Container boot counter and measurement UID

COUNT=$(cat $BASEDIR/${SCHEDID}.counter 2>/dev/null || echo 0)
COUNT=$(($COUNT + 1))
echo $COUNT > $BASEDIR/${SCHEDID}.counter

NODEID=$(</etc/nodeid)
IMAGEID=$(docker images -q --no-trunc monroe-$SCHEDID)

if [ -z "$IMAGEID" ]; then
    echo "experiment container not found."
    exit $ERROR_IMAGE_NOT_FOUND;
fi

GUID="${IMAGEID}.${SCHEDID}.${NODEID}.${COUNT}"

# replace guid in the configuration

CONFIG=$(echo $CONFIG | jq '.guid="'$GUID'"|.nodeid="'$NODEID'"')
echo $CONFIG > $BASEDIR/$SCHEDID.conf
echo "ok."

### START THE CONTAINER ###############################################

echo -n "Starting container... "
if [ -d $BASEDIR/$SCHEDID ]; then
    MOUNT_DISK="-v $BASEDIR/$SCHEDID:/monroe/results -v $BASEDIR/$SCHEDID:/outdir"
fi
if [ -d /experiments/monroe/tstat ]; then
    TSTAT_DISK="-v /experiments/monroe/tstat:/monroe/tstat:ro"
fi

# check that this container is not running yet
if [ ! -z "$(docker ps | grep monroe-$SCHEDID)" ]; then
    echo "already running."
    exit $NOERROR_CONTAINER_IS_RUNNING;
fi

# identify the monroe/noop container, running in the
# network namespace called 'monroe'
MONROE_NOOP=$(docker ps |grep monroe/noop|awk '{print $1}')
if [ -z "$MONROE_NOOP" ]; then
    echo "network context missing."
    exit $ERROR_NETWORK_CONTEXT_NOT_FOUND;
fi

if [ ! -z "$IS_SSH" ]; then
    OVERRIDE_ENTRYPOINT=" --entrypoint=dumb-init "
    OVERRIDE_PARAMETERS=" /bin/bash /usr/bin/monroe-sshtunnel-client.sh "
fi

CID_ON_START=$(docker run -d $OVERRIDE_ENTRYPOINT  \
       --name=monroe-$SCHEDID \
       --net=container:$MONROE_NOOP \
       --cap-add NET_ADMIN \
       --cap-add NET_RAW \
       -v $BASEDIR/$SCHEDID.conf:/monroe/config:ro \
       -v /etc/nodeid:/nodeid:ro \
       $MOUNT_DISK \
       $TSTAT_DISK \
       $CONTAINER $OVERRIDE_PARAMETERS)

echo "ok."

# start accounting
echo "Starting accounting."
/usr/bin/usage-defaults || true

# CID: the runtime container ID
CID=$(docker ps --no-trunc | grep $CONTAINER | awk '{print $1}' | head -n 1)

if [ -z "$CID" ]; then
    echo 'failed; container exited immediately' > $STATUSDIR/$SCHEDID.status
    echo "Container exited immediately."
    echo "Log output:"
    docker logs -t $CID_ON_START || true
    echo ""
    exit $ERROR_CONTAINER_DID_NOT_START;
fi

# PID: the container process ID
PID=$(docker inspect -f '{{.State.Pid}}' $CID)

if [ ! -z $PID ]; then
  echo "Started docker process $CID $PID."
else
  echo 'failed; container exited immediately' > $STATUSDIR/$SCHEDID.status
  echo "Container exited immediately."
  echo "Log output:"
  docker logs -t $CID_ON_START || true
  exit $ERROR_CONTAINER_DID_NOT_START;
fi

echo $PID > $BASEDIR/$SCHEDID.pid
if [ -z "$STATUS" ]; then
  echo 'started' > $STATUSDIR/$SCHEDID.status
else
  echo $STATUS > $STATUSDIR/$SCHEDID.status
fi
sysevent -t Scheduling.Task.Started -k id -v $SCHEDID
echo "Startup finished $(date)."
