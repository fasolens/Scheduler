#!/bin/bash
set -e

SCHEDID=$1
CONTAINER=monroe-$SCHEDID

ERROR_CONTAINER_DID_NOT_START=10

# make sure network namespaces are set up
mkdir -p /var/run/netns
# make sure our cgroup is setup
cgcreate -g net_cls:/monroe

### START THE CONTAINER ####################################
# TODO: parameters to be passed, e.g. nodeid
# NOTE: this assumes the container wrapper delays execution
#       until the network interfaces are available

docker run -d --cgroup-parent=monroe\
       --net=none \
       -v /outdir/$SCHEDID:/outdir \
       $CONTAINER

# CID: the runtime container ID
CID=$(docker ps --no-trunc | grep $CONTAINER | awk '{print $1}')

if [ -z "$CID" ]; then
    exit $ERROR_CONTAINER_DID_NOT_START;
fi

# PID: the container process ID
PID=$(docker inspect -f '{{.State.Pid}}' $CID)

if [ ! -z $PID ]; then
  # named the container network namespace 'monroe'
  # TODO: for passive containers, start them in the existing namespace
  #       of the same name
  rm /var/run/netns/monroe || true;
  ln -s /proc/$PID/ns/net /var/run/netns/monroe;

  # to execute any command within the monroe netns, use $MNS command
  MNS="ip netns exec monroe";

  # set container net_cls.classid foc accounting and quotas
  echo '0x00100001' > /sys/fs/cgroup/net_cls,net_prio/system.slice/monroe-${CID}.scope/net_cls.classid;

  ### TRAFFIC QUOTAS #########################################
  # we currently use net_cls 10:1 for the active experiment
  # we currently use net_cls 10:2 for passive experiments

  # TODO: pass these quotas as parameter from the scheduler
  TRAFFIC_IN=10000000;
  TRAFFIC_OUT=10000000;

  # set outgoing quota rules
  $MNS iptables -N MON_ACT_O;
  $MNS iptables -A MON_ACT_O -m quota --quota $TRAFFIC_OUT -j ACCEPT;
  $MNS iptables -A MON_ACT_O -j DROP;
  $MNS iptables -A OUTPUT -m cgroup --cgroup 0x00100001 -j MON_ACT_O;
  $MNS iptables -A OUTPUT -m cgroup --cgroup 0x00100002 -j DROP;

  # set incoming quota rules
  $MNS iptables -N MON_ACT_I;
  $MNS iptables -A MON_ACT_I -m quota --quota $TRAFFIC_IN -j ACCEPT;
  $MNS iptables -A MON_ACT_I -j DROP;
  $MNS iptables -A INPUT -m cgroup --cgroup 0x00100001 -j MON_ACT_I;
  $MNS iptables -A INPUT -m cgroup --cgroup 0x00100002 -j DROP;

  ### NETWORK INTERFACES #####################################

  # TODO: get these assigned by the scheduler
  INTERFACES="usb0 usb1 usb2 wlan0 eth0 lo";
  for IF in $INTERFACES; do
      if [ -z "$(ip link|grep $IF)" ]; then continue; fi

      ip link add link $IF montmp type macvlan;
      ip link set montmp netns monroe;
      $MNS ip link set montmp name $IF;

      # TODO: do a proper network configuration, or run multi inside the container
      $MNS ifconfig $IF up;
      $MNS multi_client -d;
   done

else
  exit 1;
fi

# TODO log status to sysevent and return a success value to the scheduler
