#!/bin/bash
set -e

SCHEDID=$1
CONTAINER_URL=$2

# TODO: Check if we have sufficient resources to deploy this container.
# If not, return an error code to delay deployment.

DISKQUOTA=10000 #K

DISKSPACE=$(df / --output=avail|tail -n1)
if (( "$DISKSPACE" < $(( 2000000 + $DISKQUOTA )) )); then
    logger -t container-deploy not enough disk space to deploy container $1;
    exit 1;
fi

docker pull $CONTAINER_URL
docker tag -f $CONTAINER_URL monroe-$SCHEDID

mkdir -p /outdir/$SCHEDID
dd if=/dev/zero of=/outdir/${SCHEDID}.disk bs=1000 count=$DISKQUOTA
mkfs /outdir/${SCHEDID}.disk
mount -o loop /outdir/${SCHEDID}.disk /outdir/${SCHEDID}
