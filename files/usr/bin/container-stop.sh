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

CID=$( docker ps -a | grep $CONTAINER | awk '{print $1}' )

if [ $(docker inspect -f "{{.State.Running}}" $CID 2>/dev/null) ]; then
  docker stop --time=10 $CID;
else
  echo "Container is no longer running.";
fi

if [ -d $BASEDIR/$SCHEDID ]; then
  docker logs -t $CID &> $BASEDIR/$SCHEDID/container.log
  for i in $(ls $USAGEDIR/monroe-$SCHEDID/*|sort); do
    LINE=$(echo -e "$(basename $i)\t\t$(cat $i)")
    echo $LINE >> $STATUSDIR/$SCHEDID.traffic;
    echo $LINE >> $BASEDIR/$SCHEDID/container.stat;
  done
fi

if [ -z "$STATUS" ]; then
  echo 'stopped' > $STATUSDIR/$SCHEDID.status;
else
  echo $STATUS > $STATUSDIR/$SCHEDID.status;
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

if [ ! -z "$IS_INTERNAL" ]; then
    monroe-rsync-results;
    rm $BASEDIR/$SCHEDID/container.*
else
    monroe-user-experiments;  #rsync, if possible
fi

rm -r $BASEDIR/$SCHEDID/tmp*       # remove any tmp files
rm -r $BASEDIR/$SCHEDID/*.tmp
rm -r $BASEDIR/$SCHEDID/lost+found # remove lost+found created by fsck
# any other file should be rsynced by now

if [ ! $(ls -A $BASEDIR/$SCHEDID/ 2>/dev/null) ]; then
  umount $BASEDIR/$SCHEDID            2>/dev/null  || echo 'Directory is no longer mounted.'
  rmdir  $BASEDIR/$SCHEDID            2>/dev/null
  rm     $BASEDIR/${SCHEDID}.conf     2>/dev/null
  rm     $STATUSDIR/${SCHEDID}.conf   2>/dev/null
  rm     $BASEDIR/${SCHEDID}.disk     2>/dev/null
  rm     $BASEDIR/${SCHEDID}.counter  2>/dev/null
  rm -r  $USAGEDIR/monroe-${SCHEDID}  2>/dev/null
fi
rm     $BASEDIR/${SCHEDID}.pid      2>/dev/null
