#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #777"
echo "======================="

check_running() {
  name=$1;
  # echo "check_running $name"
  ps -C $name
  if [ "$?" -ne "0" ]
  then
    echo "Process $name does not exist"
    exit 1;
  fi
}

check_killed() {
  name=$1;
  # echo "check_killed $name"
  ps -C $name
  if [ "$?" -eq "0" ]
  then
    echo "Process $name still exists"
    exit 1;
  fi
}

# only start servers if they're not yet running
$HT_HOME/bin/serverup rangeserver
if [ "$?" -ne "0" ]
then
    $HT_HOME/bin/start-test-servers.sh --clear
fi

# no options: stop everything
$HT_HOME/bin/stop-servers.sh
check_killed  Hypertable.Master
check_killed  Hypertable.RangeServer
check_killed  Hyperspace.Master
check_killed  ThriftBroker
check_killed  localBroker

$HT_HOME/bin/start-test-servers.sh

# make sure they're all running
check_running Hypertable.Master
check_running Hypertable.RangeServer
check_running Hyperspace.Master
check_running ThriftBroker
check_running localBroker

$HT_HOME/bin/stop-servers.sh thriftbroker
check_running Hypertable.Master
check_running Hypertable.RangeServer
check_running Hyperspace.Master
check_killed  ThriftBroker
check_running localBroker

$HT_HOME/bin/stop-servers.sh thriftbroker master
check_killed  Hypertable.Master
check_running Hypertable.RangeServer
check_running Hyperspace.Master
check_killed  ThriftBroker
check_running localBroker

$HT_HOME/bin/stop-servers.sh rangeserver
check_killed  Hypertable.Master
check_killed  Hypertable.RangeServer
check_running Hyperspace.Master
check_killed  ThriftBroker
check_running localBroker

$HT_HOME/bin/stop-servers.sh hyperspace dfsbroker
check_killed  Hypertable.Master
check_killed  Hypertable.RangeServer
check_killed  Hyperspace.Master
check_killed  ThriftBroker
check_killed  localBroker

echo "SUCCESS"
