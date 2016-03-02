#!/bin/bash

SCHEDID=$1
CONTAINER=monroe-$SCHEDID

# Stop our container and ensure the interfaces are still up

# tags don't work with docker stop :|
CID=$( docker ps | grep $CONTAINER | awk '{print $1}' )
if [ -z "$CID" ]; then
  #exit 1;
  echo 1;
else
  docker stop --time=10 $CID;
fi

# TODO sync outdir

# when in production, we will also want to delete the expired container image
# but only if no recurring experiments are scheduled

# tags don't work with docker rmi either :|
REF=$( docker images | grep $CONTAINER | awk '{print $3}' )
docker rmi -f $REF

# undo startup and deployment steps
MNS="ip netns exec monroe"

INTERFACES="usb0 usb1 usb2 wlan0 eth0";
for IF in $INTERFACES; do
  $MNS ip link delete $IF;
done

ip netns delete monroe

umount /outdir/$SCHEDID
rmdir  /outdir/$SCHEDID
rm     /outdir/${SCHEDID}.disk
