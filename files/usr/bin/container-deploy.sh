#!/bin/bash
set -e

SCHEDID=$1
CONTAINER_URL=$2

ERROR_CONTAINER_NOT_FOUND=100
ERROR_INSUFFICIENT_DISK_SPACE=101

# TODO: Check if we have sufficient resources to deploy this container.
# If not, return an error code to delay deployment.

DISKQUOTA=10000 #K

DISKSPACE=$(df / --output=avail|tail -n1)
if (( "$DISKSPACE" < $(( 2000000 + $DISKQUOTA )) )); then
    logger -t container-deploy not enough disk space to deploy container $1;
    exit ERROR_INSUFFICIENT_DISK_SPACE;
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
    dd if=/dev/zero of=/outdir/${SCHEDID}.disk bs=1000 count=$DISKQUOTA;
    mkfs /outdir/${SCHEDID}.disk;
    mount -o loop /outdir/${SCHEDID}.disk /outdir/${SCHEDID};
fi
