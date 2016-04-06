#!/bin/bash
set -e

SCHEDID=$1

ERROR_CONTAINER_NOT_FOUND=100
ERROR_INSUFFICIENT_DISK_SPACE=101

# TODO: Check if we have sufficient resources to deploy this container.
# If not, return an error code to delay deployment.

if [ -f /outdir/$SCHEDID.conf ]; then
  CONFIG=$(cat /outdir/$1.conf);
  QUOTA_DISK=$(echo $CONFIG | jq .storage);
  CONTAINER_URL=$(echo $CONFIG | jq .script);
fi
if [ -z "$QUOTA_DISK" ]; then
  QUOTA_DISK=500000; #KB!
else 
  QUOTA_DISK=$(( $QUOTA_DISK / 1000 ))
fi;

DISKSPACE=$(df / --output=avail|tail -n1)
if (( "$DISKSPACE" < $(( 2000000 + $QUOTA_DISK )) )); then
    logger -t container-deploy not enough disk space to deploy container $1;
    exit $ERROR_INSUFFICIENT_DISK_SPACE;
fi

EXISTED=$(docker images -q $CONTAINER_URL)
docker pull $CONTAINER_URL || exit $ERROR_CONTAINER_NOT_FOUND
#retag container image with scheduling id
docker tag $CONTAINER_URL monroe-$SCHEDID
if [ -z "$EXISTED" ]; then
    docker rmi $CONTAINER_URL
fi

if [ ! -d /outdir/$SCHEDID ]; then 
    mkdir -p /outdir/$SCHEDID;
    dd if=/dev/zero of=/outdir/${SCHEDID}.disk bs=1000 count=$QUOTA_DISK;
    mkfs /outdir/${SCHEDID}.disk;
    mount -o loop /outdir/${SCHEDID}.disk /outdir/${SCHEDID};
fi
