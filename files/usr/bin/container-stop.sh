#!/bin/bash
#This script should always be run, as long as the container is deployed.

SCHEDID=$1
CONTAINER=monroe-$SCHEDID

# undo startup and deployment steps
MNS="ip netns exec monroe"

INTERFACES="usb0 usb1 usb2 wlan0 eth0";
for IF in $INTERFACES; do
  if [ -z "$($MNS ip link|grep $IF)" ]; then continue; fi
  $MNS ip link delete $IF;
done

ip netns delete monroe

# Stop the container if it is running.
CID=$( docker ps | grep $CONTAINER | awk '{print $1}' )
if [ -z "$CID" ]; then
  echo "Container is no longer running.";
else
  docker stop --time=10 $CID;
fi

echo 'finished' > /outdir/$SCHEDID.status

# TODO sync outdir

# when in production, we will also want to delete the expired container image
# but only if no scheduled experiments are expected to use the same image

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

umount /outdir/$SCHEDID
rmdir  /outdir/$SCHEDID
rm     /outdir/${SCHEDID}.disk
rm     /outdir/${SCHEDID}.counter

