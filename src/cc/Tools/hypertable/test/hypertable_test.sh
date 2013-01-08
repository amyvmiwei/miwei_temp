#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================================"
echo "Hypertable shell test"
echo "======================================"
$HT_HOME/bin/start-test-servers.sh --clear

./hypertable_test

