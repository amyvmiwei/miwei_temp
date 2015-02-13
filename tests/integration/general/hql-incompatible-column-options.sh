#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/ht-start-test-servers.sh --clear --no-thriftbroker

echo "create table foo (a COUNTER TIME_ORDER DESC);" | $HT_HOME/bin/ht shell --batch
if [ $? -eq 0 ]; then
    $HT_HOME/bin/ht-stop-servers.sh
    echo "Column options COUNTER and TIME_ORDER DESC should not have been allowed!"
    exit 1
fi

echo "create table foo (a COUNTER MAX_VERSIONS 1);" | $HT_HOME/bin/ht shell --batch
if [ $? -eq 0 ]; then
    $HT_HOME/bin/ht-stop-servers.sh
    echo "Column options COUNTER and MAX_VERSIONS 1 should not have been allowed!"
    exit 1
fi

$HT_HOME/bin/ht-stop-servers.sh
exit 0

