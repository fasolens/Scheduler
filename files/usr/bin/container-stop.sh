#!/bin/bash
#This script should always be run, as long as the container is deployed.

SCHEDID=$1
STATUS=$2
CONTAINER=monroe-$SCHEDID

BASEDIR=/experiments/user
STATUSDIR=$BASEDIR
USAGEDIR=/monroe/usage/netns

if [ -f $BASEDIR/$SCHEDID.conf ]; then
  CONFIG=$(cat $BASEDIR/$SCHEDID.conf);
  IS_INTERNAL=$(echo $CONFIG | jq -r '.internal // empty');
  BDEXT=$(echo $CONFIG | jq -r '.basedir // empty');
fi
if [ ! -z "$IS_INTERNAL" ]; then
  BASEDIR=/experiments/monroe${BDEXT}
fi
exec > /tmp/cleanup.log 2>&1

CID=$( docker ps -a | grep $CONTAINER | awk '{print $1}' )

echo "Finalize accounting."
/usr/bin/usage-defaults

echo -n "Stopping container... "
if [ $(docker inspect -f "{{.State.Running}}" $CID 2>/dev/null) ]; then
  docker stop --time=10 $CID;
  echo "stopped:"
  docker inspect $CID|jq .[].State
else
  echo "Container is no longer running.";
fi
sysevent -t Scheduling.Task.Stopped -k id -v $SCHEDID

if [ -d $BASEDIR/$SCHEDID ]; then
  echo "Retrieving container logs:"
  if [ ! -z "$CID" ]; then
    docker logs -t $CID &> $STATUSDIR/$SCHEDID/container.log;
  else
    echo "CID not found for $CONTAINER." > $STATUSDIR/$SCHEDID/container.log;
  fi
  echo ""

  echo -n "Syncing traffic statistics... "
  monroe-user-experiments;
  TRAFFIC=$(cat $STATUSDIR/$SCHEDID.traffic)

  for i in $(ls $USAGEDIR/monroe-$SCHEDID/*.rx.total|sort); do
    MACICCID=$(basename $i | sed -e 's/\..*//g')
    TRAFFIC=$(echo "$TRAFFIC" | jq ".interfaces.\"$MACICCID\"=$(cat $USAGEDIR/monroe-$SCHEDID/$MACICCID.total)")
  done;
  if [ ! -z "$TRAFFIC" ]; then
    echo "$TRAFFIC" > $STATUSDIR/$SCHEDID.traffic
    echo "$TRAFFIC" > $STATUSDIR/$SCHEDID/container.stat
  fi
  echo "ok."
fi

if [ -z "$STATUS" ]; then
  echo 'stopped' > $STATUSDIR/$SCHEDID.status;
else
  echo $STATUS > $STATUSDIR/$SCHEDID.status;
fi

echo -n "Untagging container image... "
REF=$( docker images | grep $CONTAINER | awk '{print $3}' )
if [ -z "$REF" ]; then
  echo "Container is no longer deployed.";
else
  docker rmi -f $CONTAINER
fi
echo "ok."

echo -n "Cleaning unused container images... "
# remove all stopped containers (remove all, ignore errors when running)
docker rm $(docker ps -aq) 2>/dev/null
# clean any untagged containers without dependencies (unused layers)
docker rmi $(docker images -a|grep '^<none>'|awk "{print \$3}") 2>/dev/null
echo "ok."

echo -n "Syncing results... "
if [ ! -z "$IS_INTERNAL" ]; then
    monroe-rsync-results;
    rm $BASEDIR/$SCHEDID/container.*
else
    cat /tmp/cleanup.log > $BASEDIR/$SCHEDID/cleanup.log
    echo "(end of public log)"
    monroe-user-experiments;  #rsync all remaining files
fi
echo "ok."

echo -n "Cleaning files... "
rm -r $BASEDIR/$SCHEDID/tmp*       # remove any tmp files
rm -r $BASEDIR/$SCHEDID/*.tmp
rm -r $BASEDIR/$SCHEDID/lost+found # remove lost+found created by fsck
# any other file should be rsynced by now

umount $BASEDIR/$SCHEDID            
rmdir  $BASEDIR/$SCHEDID            
rm     $BASEDIR/${SCHEDID}.conf     
rm     $STATUSDIR/${SCHEDID}.conf   
rm     $BASEDIR/${SCHEDID}.disk     
rm     $BASEDIR/${SCHEDID}.counter  
rm -r  $USAGEDIR/monroe-${SCHEDID}  
cp     $STATUSDIR/${SCHEDID}.traffic  $STATUSDIR/${SCHEDID}.traffic_ 
fi
rm     $BASEDIR/${SCHEDID}.pid
echo "ok."
echo "Cleanup finished $(date)."
