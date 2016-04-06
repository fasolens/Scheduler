#!/bin/bash
#This script should always be run, as long as the container is deployed.

SCHEDID=$1
STATUS=$2
CONTAINER=monroe-$SCHEDID

CID=$( docker ps | grep $CONTAINER | awk '{print $1}' )
if [ -z "$CID" ]; then
  echo "Container is no longer running.";
else
  MNS="ip netns exec monroe";

  INTERFACES="usb0 usb1 usb2 wlan0 eth0";
  for IF in $INTERFACES; do
    if [ -z "$($MNS ip link|grep $IF)" ]; then continue; fi
    $MNS ip link delete $IF;
  done

  ip netns delete monroe;

  # Stop the container if it is running.
  docker stop --time=10 $CID;
fi

if [ -z "$STATUS" ]; then
  echo 'finished' > /outdir/$SCHEDID.status;
else 
  echo $STATUS > /outdir/$SCHEDID.status;
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

umount /outdir/$SCHEDID            2>/dev/null  || echo 'Directory is no longer mounted.'
rmdir  /outdir/$SCHEDID            2>/dev/null   
rm     /outdir/${SCHEDID}.disk     2>/dev/null 
rm     /outdir/${SCHEDID}.counter  2>/dev/null
rm     /outdir/${SCHEDID}.conf     2>/dev/null

