#!/bin/bash
#This script should always be run, as long as the container is deployed.

SCHEDID=$1
STATUS=$2
CONTAINER=monroe-$SCHEDID

BASEDIR=/experiments/user
if [ -f $BASEDIR/$SCHEDID.conf ]; then
  CONFIG=$(cat $BASEDIR/$SCHEDID.conf);
  IS_INTERNAL=$(echo $CONFIG | jq -r '.internal // empty');
  BASEDIR=$(echo $CONFIG | jq -r '.basedir // empty');
fi
if [ ! -z "$IS_INTERNAL" ]; then
  BASEDIR=/experiments/monroe${BASEDIR}
fi

CID=$( docker ps | grep $CONTAINER | awk '{print $1}' )
if [ -z "$CID" ]; then
  echo "Container is no longer running.";
else
  # Stop the container if it is running.
  docker stop --time=10 $CID;
fi

if [ -z "$STATUS" ]; then
  echo 'stopped' > $BASEDIR/$SCHEDID.status;
else
  echo $STATUS > $BASEDIR/$SCHEDID.status;
fi

REF=$( docker images | grep $CONTAINER | awk '{print $3}' )
if [ -z "$REF" ]; then
  echo "Container is no longer deployed.";
else
  docker rmi -f $CONTAINER
fi

# remove all stopped containers (remove all, ignore errors when running)
docker rm $(docker ps -aq) 2>/dev/null
# clean any untagged containers without dependencies (unused layers)
docker rmi $(docker images -a|grep '^<none>'|awk "{print \$3}") 2>/dev/null

# TODO sync outdir

umount $BASEDIR/$SCHEDID            2>/dev/null  || echo 'Directory is no longer mounted.'
rmdir  $BASEDIR/$SCHEDID            2>/dev/null
rm     $BASEDIR/${SCHEDID}.disk     2>/dev/null
rm     $BASEDIR/${SCHEDID}.counter  2>/dev/null
rm     $BASEDIR/${SCHEDID}.conf     2>/dev/null # FIXME: use original basedir
rm     $BASEDIR/${SCHEDID}.pid      2>/dev/null
