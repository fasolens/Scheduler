#!/bin/bash
set -e

SCHEDID=$1
STATUS=$2
CONTAINER=monroe-$SCHEDID

BASEDIR=/experiments/user
mkdir -p $BASEDIR

if [ -f $BASEDIR/$SCHEDID.conf ]; then
  CONFIG=$(cat $BASEDIR/$SCHEDID.conf);
  IS_INTERNAL=$(echo $CONFIG | jq -r .internal);
  BASEDIR=$(echo $CONFIG | jq -r .basedir);
fi
if [ ! -z "$IS_INTERNAL" ]; then
  BASEDIR=/experiments/monroe${BASEDIR}
fi
mkdir -p $BASEDIR

NOERROR_CONTAINER_IS_RUNNING=0

ERROR_CONTAINER_DID_NOT_START=10
ERROR_NETWORK_CONTEXT_NOT_FOUND=11
ERROR_IMAGE_NOT_FOUND=12

# make sure network namespaces are set up
mkdir -p /var/run/netns

# Container boot counter and measurement UID

COUNT=$(cat $BASEDIR/${SCHEDID}.counter 2>/dev/null || echo 0)
COUNT=$(($COUNT + 1))
echo $COUNT > $BASEDIR/${SCHEDID}.counter

NODEID=$(</etc/nodeid)
IMAGEID=$(docker images -q --no-trunc monroe-$SCHEDID)

if [ -z "$IMAGEID"]; then
    exit $ERROR_IMAGE_NOT_FOUND;
fi

GUID="${IMAGEID}.${SCHEDID}.${NODEID}.${COUNT}"

# replace guid in the configuration

CONFIG=$(echo $CONFIG | jq '.guid="'$GUID'"')
echo $CONFIG > $BASEDIR/$SCHEDID.conf

### START THE CONTAINER ###############################################

if [ -d $BASEDIR/$SCHEDID ]; then
    MOUNT_DISK="-v $BASEDIR/$SCHEDID:/monroe/results -v $BASEDIR/$SCHEDID:/outdir"
fi

# check that this container is not running yet
if [ ! -z "$(docker ps | grep monroe-$SCHEDID)" ]; then
    exit $NOERROR_CONTAINER_IS_RUNNING;
fi

# identify the monroe/noop container, running in the
# network namespace called 'monroe'
MONROE_NOOP=$(docker ps |grep monroe/noop|awk '{print $1}')
if [ -z "$MONROE_NOOP" ]; then
    exit ERROR_NETWORK_CONTEXT_NOT_FOUND;
fi

docker run -d \
#       --net=container:$MONROE_NOOP \
       --net=host \
       --cap-add NET_ADMIN \
       --cap-add NET_RAW \
       -v $BASEDIR/$SCHEDID.conf:/monroe/config:ro \
       $MOUNT_DISK \
       $CONTAINER

# CID: the runtime container ID
CID=$(docker ps --no-trunc | grep $CONTAINER | awk '{print $1}' | head -n 1)

if [ -z "$CID" ]; then
    echo 'failed' > $BASEDIR/$SCHEDID.status
    exit $ERROR_CONTAINER_DID_NOT_START;
fi

# PID: the container process ID
PID=$(docker inspect -f '{{.State.Pid}}' $CID)

if [ ! -z $PID ]; then
  echo "Started docker process $CID $PID."
else
  echo 'failed' > $BASEDIR/$SCHEDID.status
  exit $ERROR_CONTAINER_DID_NOT_START;
fi

echo $PID > $BASEDIR/$SCHEDID.pid
if [ -z "$STATUS" ]; then
  echo 'started' > $BASEDIR/$SCHEDID.status
else
  echo $STATUS > $BASEDIR/$SCHEDID.status
fi
