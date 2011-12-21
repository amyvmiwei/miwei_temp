#!/usr/bin/env bash

HT_TEST_DFS=${HT_TEST_DFS:-local}

set -v

TEST_BIN=./client_test
HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}

$HT_HOME/bin/start-test-servers.sh --clean

sleep 5;
$HT_HOME/bin/stop-servers.sh --no-thriftbroker 
sleep 3;
$HT_HOME/bin/start-all-servers.sh --no-thriftbroker $HT_TEST_DFS
sleep 12;

cd ${THRIFT_CPP_TEST_DIR};
${TEST_BIN}
