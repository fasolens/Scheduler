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
  BDEXT=$(echo $CONFIG | jq -r '.basedir // empty');
fi
if [ ! -z "$IS_INTERNAL" ]; then
  BASEDIR=/experiments/monroe${BDEXT}
  mkdir -p $BASEDIR
  echo $CONFIG > $BASEDIR/$SCHEDID.conf
fi

NOERROR_CONTAINER_IS_RUNNING=0

ERROR_CONTAINER_DID_NOT_START=10
ERROR_NETWORK_CONTEXT_NOT_FOUND=11
ERROR_IMAGE_NOT_FOUND=12
ERROR_MAINTENANCE_MODE=13

# Check for maintenance mode
MAINTENANCE=$(cat /.maintenance || echo 0)
if [ $MAINTENANCE -eq 1 ]; then
   echo 'failed; node is in maintenance mode.' > $STATUSDIR/$SCHEDID.status
   exit $ERROR_MAINTENANCE_MODE;
fi

# make sure network namespaces are set up
mkdir -p /var/run/netns

# Container boot counter and measurement UID

COUNT=$(cat $BASEDIR/${SCHEDID}.counter 2>/dev/null || echo 0)
COUNT=$(($COUNT + 1))
echo $COUNT > $BASEDIR/${SCHEDID}.counter

NODEID=$(</etc/nodeid)
IMAGEID=$(docker images -q --no-trunc monroe-$SCHEDID)

if [ -z "$IMAGEID" ]; then
    exit $ERROR_IMAGE_NOT_FOUND;
fi

GUID="${IMAGEID}.${SCHEDID}.${NODEID}.${COUNT}"

# replace guid in the configuration

CONFIG=$(echo $CONFIG | jq '.guid="'$GUID'"|.nodeid="'$NODEID'"')
echo $CONFIG > $BASEDIR/$SCHEDID.conf

### START THE CONTAINER ###############################################

if [ -d $BASEDIR/$SCHEDID ]; then
    MOUNT_DISK="-v $BASEDIR/$SCHEDID:/monroe/results -v $BASEDIR/$SCHEDID:/outdir"
fi
if [ -d /experiments/monroe/tstat ]; then
    TSTAT_DISK="-v /experiments/monroe/tstat:/monroe/tstat:ro"
fi

# check that this container is not running yet
if [ ! -z "$(docker ps | grep monroe-$SCHEDID)" ]; then
    exit $NOERROR_CONTAINER_IS_RUNNING;
fi

# identify the monroe/noop container, running in the
# network namespace called 'monroe'
MONROE_NOOP=$(docker ps |grep monroe/noop|awk '{print $1}')
if [ -z "$MONROE_NOOP" ]; then
    exit $ERROR_NETWORK_CONTEXT_NOT_FOUND;
fi

docker run -d \
       --name=monroe-$SCHEDID \
       --net=container:$MONROE_NOOP \
       --cap-add NET_ADMIN \
       --cap-add NET_RAW \
       -v $BASEDIR/$SCHEDID.conf:/monroe/config:ro \
       -v /etc/nodeid:/nodeid:ro \
       $MOUNT_DISK \
       $TSTAT_DISK \
       $CONTAINER

# sync usage monitoring
/usr/bin/usage /monroe/usage/netns /tmp/metadata-usage-monroe eth0 wlan0 wlan1 usb0 usb1 usb2 || true

# CID: the runtime container ID
CID=$(docker ps --no-trunc | grep $CONTAINER | awk '{print $1}' | head -n 1)

if [ -z "$CID" ]; then
    echo 'failed; container exited immediately' > $STATUSDIR/$SCHEDID.status
    exit $ERROR_CONTAINER_DID_NOT_START;
fi

# PID: the container process ID
PID=$(docker inspect -f '{{.State.Pid}}' $CID)

if [ ! -z $PID ]; then
  echo "Started docker process $CID $PID."
else
  echo 'failed; container exited immediately' > $STATUSDIR/$SCHEDID.status
  exit $ERROR_CONTAINER_DID_NOT_START;
fi

echo $PID > $BASEDIR/$SCHEDID.pid
if [ -z "$STATUS" ]; then
  echo 'started' > $STATUSDIR/$SCHEDID.status
else
  echo $STATUS > $STATUSDIR/$SCHEDID.status
fi
