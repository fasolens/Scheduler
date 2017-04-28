#!/bin/bash

echo "redirecting all output to the following locations:"
echo " - /tmp/container-deploy until an experiment directory is created"
echo " - experiment/deploy.log after that."

rm /tmp/container-deploy 2>/dev/null 
exec > /tmp/container-deploy 2>&1
set -e

SCHEDID=$1
CONTAINER_URL=$2 # may be empty, just for convenience of starting manually.

BASEDIR=/experiments/user
STATUSDIR=$BASEDIR
mkdir -p $BASEDIR

ERROR_CONTAINER_NOT_FOUND=100
ERROR_INSUFFICIENT_DISK_SPACE=101
ERROR_QUOTA_EXCEEDED=102
ERROR_MAINTENANCE_MODE=103

echo -n "Checking for maintenance mode... "
MAINTENANCE=$(cat /monroe/maintenance/enabled || echo 0)
if [ $MAINTENANCE -eq 1 ]; then
  echo "enabled."
  exit $ERROR_MAINTENANCE_MODE; 
fi
echo "disabled."


# Check if we have sufficient resources to deploy this container.
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
  QUOTA_DISK=10000000; 
fi;
QUOTA_DISK_KB=$(( $QUOTA_DISK / 1000 ))

echo -n "Checking for disk space... "
DISKSPACE=$(df /var/lib/docker --output=avail|tail -n1)
if (( "$DISKSPACE" < $(( 100000 + $QUOTA_DISK_KB )) )); then
    logger -t container-deploy not enough disk space to deploy container $1;
    exit $ERROR_INSUFFICIENT_DISK_SPACE;
fi
echo "ok."

EXISTED=$(docker images -q $CONTAINER_URL)

# TODO: check if exists, restrict to only this process
iptables -w -I OUTPUT 1 -p tcp --destination-port 443 -m owner --gid-owner 0 -j ACCEPT
iptables -w -Z OUTPUT 1
iptables -w -I INPUT 1 -p tcp --source-port 443 -j ACCEPT
iptables -w -Z INPUT 1
trap "iptables -w -D OUTPUT -p tcp --destination-port 443 -m owner --gid-owner 0 -j ACCEPT; iptables -w -D INPUT  -p tcp --source-port 443 -j ACCEPT" EXIT

echo -n "Pulling container... "
timeout 120 docker pull $CONTAINER_URL || {
  echo "exit code $?";
  exit $ERROR_CONTAINER_NOT_FOUND;
}

SENT=$(iptables -vxL OUTPUT 1 | awk '{print $2}')
RECEIVED=$(iptables -vxL INPUT 1 | awk '{print $2}')
SUM=$(($SENT + $RECEIVED))

#retag container image with scheduling id
docker tag $CONTAINER_URL monroe-$SCHEDID
if [ -z "$EXISTED" ]; then
    docker rmi $CONTAINER_URL
fi

#check if storage quota is exceeded - should never happen
if [ "$SUM" -gt "$QUOTA_DISK" ]; then
  docker rmi monroe-$SCHEDID || true;
  echo  "quota exceeded ($SUM)."
  exit $ERROR_QUOTA_EXCEEDED;
fi
echo  "ok."

echo -n "Creating file system... "

EXPDIR=$BASEDIR/$SCHEDID
if [ ! -d $EXPDIR ]; then
    mkdir -p $EXPDIR;
    dd if=/dev/zero of=$EXPDIR.disk bs=1000 count=$QUOTA_DISK_KB;
    mkfs.ext4 $EXPDIR.disk -F -L $SCHEDID;
fi
mountpoint -q $EXPDIR || {
    mount -t ext4 -o loop,data=journal,nodelalloc,barrier=1 $EXPDIR.disk $EXPDIR;
}
JSON=$( echo '{}' | jq .deployment=$SUM )
echo $JSON > $STATUSDIR/$SCHEDID.traffic
echo "ok."

echo "Deployment finished $(date)".
# moving deployment files and switching redirects
cat /tmp/container-deploy >> $EXPDIR/deploy.log
rm /tmp/container-deploy
