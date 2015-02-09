#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #777"
echo "======================="

check_running() {
  name=$1;
  # echo "check_running $name"
  ps auxww | fgrep "$name" | fgrep -v cronolog | fgrep -v grep
  if [ "$?" -ne "0" ]
  then
    echo "Process $name does not exist"
    exit 1;
  fi
}

check_killed() {
  name=$1;
  # echo "check_killed $name"
  ps auxww | fgrep "$name" | fgrep -v cronolog | fgrep -v grep
  if [ "$?" -eq "0" ]
  then
    echo "Process $name still exists"
    exit 1;
  fi
}

# only start servers if they're not yet running
$HT_HOME/bin/ht-check-rangeserver.sh
if [ $? -ne 0 ]; then
    $HT_HOME/bin/ht-start-test-servers.sh --clear
fi

# no options: stop everything
$HT_HOME/bin/ht-stop-servers.sh
check_killed  htMaster
check_killed  htRangeServer
check_killed  htHyperspace
check_killed  htThriftBroker
check_killed  htFsBrokerLocal

$HT_HOME/bin/ht-start-test-servers.sh

# make sure they're all running
check_running htMaster
check_running htRangeServer
check_running htHyperspace
check_running htThriftBroker
check_running htFsBrokerLocal

$HT_HOME/bin/ht-stop-servers.sh thriftbroker
check_running htMaster
check_running htRangeServer
check_running htHyperspace
check_killed  htThriftBroker
check_running htFsBrokerLocal

$HT_HOME/bin/ht-stop-servers.sh thriftbroker master
check_killed  htMaster
check_running htRangeServer
check_running htHyperspace
check_killed  htThriftBroker
check_running htFsBrokerLocal

$HT_HOME/bin/ht-stop-servers.sh rangeserver
sleep 5
check_killed  htMaster
check_killed  htRangeServer
check_running htHyperspace
check_killed  htThriftBroker
check_running htFsBrokerLocal

$HT_HOME/bin/ht-stop-servers.sh hyperspace dfsbroker
check_killed  htMaster
check_killed  htRangeServer
check_killed  htHyperspace
check_killed  htThriftBroker
check_killed  htFsBrokerLocal

echo "SUCCESS"
