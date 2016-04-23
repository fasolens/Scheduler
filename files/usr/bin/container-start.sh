#!/bin/bash
set -e

SCHEDID=$1
CONTAINER=monroe-$SCHEDID

BASEDIR=/experiments/user

if [ -f $BASEDIR/$SCHEDID.conf ]; then
  CONFIG=$(cat $BASEDIR/$1.conf);
fi

ERROR_CONTAINER_DID_NOT_START=10
ERROR_NETWORK_CONTEXT_NOT_FOUND=11

# make sure network namespaces are set up
mkdir -p /var/run/netns

# Container boot counter and measurement UID

COUNT=$(cat $BASEDIR/${SCHEDID}.counter 2>/dev/null || echo 0)
COUNT=$(($COUNT + 1))
echo $COUNT > $BASEDIR/${SCHEDID}.counter

NODEID=$(</etc/nodeid)

### START THE CONTAINER ###############################################

if [ -d $BASEDIR/$SCHEDID ]; then
    MOUNT_DISK="-v $BASEDIR/$SCHEDID:$BASEDIR"
fi

# identify the monroe/noop container, running in the
# network namespace called 'monroe'
MONROE_NOOP=$(docker ps |grep monroe/noop|awk '{print $1}')
if [ -z "$MONROE_NOOP" ]; then
    exit ERROR_NETWORK_CONTEXT_NOT_FOUND;
fi

docker run -d \
       --net=container:$MONROE_NOOP \
       --cap-add NET_ADMIN \
       --cap-add NET_RAW \
       $MOUNT_DISK \
       $CONTAINER \
       --guid ${SCHEDID}.${NODEID}.${COUNT}

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

echo 'started' > $BASEDIR/$SCHEDID.status
# TODO log status to sysevent and return a success value to the scheduler
