#!/bin/bash
set -e

SCHEDID=$1
CONTAINER_URL=$2 # may be empty, just for convenience of starting manually.

BASEDIR=/experiments/user
mkdir -p $BASEDIR

ERROR_CONTAINER_NOT_FOUND=100
ERROR_INSUFFICIENT_DISK_SPACE=101

# TODO: Check if we have sufficient resources to deploy this container.
# If not, return an error code to delay deployment.

if [ -f $BASEDIR/$SCHEDID.conf ]; then
  CONFIG=$(cat $BASEDIR/$1.conf);
  QUOTA_DISK=$(echo $CONFIG | jq -r .storage);
  CONTAINER_URL=$(echo $CONFIG | jq -r .script);
  IS_INTERNAL=$(echo $CONFIG | jq -r '.internal // empty');
  BDEXT=$(echo $CONFIG | jq -r '.basedir // empty');
fi
if [ ! -z "$IS_INTERNAL" ]; then
  BASEDIR=/experiments/monroe${BDEXT}
fi
mkdir -p $BASEDIR

if [ -z "$QUOTA_DISK" ]; then
  QUOTA_DISK=10000; #KB!
else
  QUOTA_DISK=$(( $QUOTA_DISK / 1000 ))
fi;

DISKSPACE=$(df / --output=avail|tail -n1)
if (( "$DISKSPACE" < $(( 2000000 + $QUOTA_DISK )) )); then
    logger -t container-deploy not enough disk space to deploy container $1;
    exit $ERROR_INSUFFICIENT_DISK_SPACE;
fi

EXISTED=$(docker images -q $CONTAINER_URL)

# TODO: check if exists, restrict to only this process
iptables -I OUTPUT 1 -p tcp --destination-port 443 -m owner --gid-owner 0 -j ACCEPT
iptables -Z OUTPUT 1
iptables -I INPUT 1 -p tcp --source-port 443 -j ACCEPT
iptables -Z INPUT 1

docker pull $CONTAINER_URL || exit $ERROR_CONTAINER_NOT_FOUND

SENT=$(iptables -vxL OUTPUT | grep "https" | awk '{print $2}')
RECEIVED=$(iptables -vxL INPUT | grep "https" | awk '{print $2}')
SUM=$(($SENT + $RECEIVED))

iptables -D OUTPUT -p tcp --destination-port 443 -m owner --gid-owner 0 -j ACCEPT
iptables -D INPUT  -p tcp --source-port 443 -j ACCEPT

SIZE=$(container-size $CONTAINER_URL)

# TODO: check if storage quota is exceeded - should never happen
# TODO: store container size in a recoverable place

#retag container image with scheduling id
docker tag $CONTAINER_URL monroe-$SCHEDID
if [ -z "$EXISTED" ]; then
    docker rmi $CONTAINER_URL
fi

if [ ! -d $BASEDIR/$SCHEDID ]; then
    mkdir -p $BASEDIR/$SCHEDID;
    dd if=/dev/zero of=$BASEDIR/${SCHEDID}.disk bs=1000 count=$QUOTA_DISK;
    mkfs.ext4 $BASEDIR/${SCHEDID}.disk -F -L $SCHEDID;
fi
mountpoint -q $BASEDIR/$SCHEDID || {
    mount -t ext4 -o loop,data=journal,nodelalloc,barrier=1 $BASEDIR/${SCHEDID}.disk $BASEDIR/${SCHEDID};
}

JSON=$( echo '{}' | jq .deployment=$SUM )
JSON=$( echo "$JSON" | jq ".container=\"$SIZE\"" )

echo $JSON >> $BASEDIR/$SCHEDID.traffic
