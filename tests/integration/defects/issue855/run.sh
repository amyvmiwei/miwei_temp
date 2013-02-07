#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #855"
echo "======================="

# restart servers with a clean hyperspace
$HT_HOME/bin/stop-servers.sh
$HT_HOME/bin/start-test-servers.sh --clear --no-rangeserver \
    --no-thriftbroker --no-master

echo "dump /;" | $HT_HOME/bin/ht hyperspace \
    --induce-failure=bad-hyperspace-version:signal:0 \
    --no-prompt --test-mode &> test.output1
echo "dump /;" | $HT_HOME/bin/ht hyperspace \
    --no-prompt --test-mode &> test.output2

diff test.output1 ${SCRIPT_DIR}/test.golden1
if [ $? -ne "0" ]
then
  echo "FAIL - golden file differs, exiting"
  exit 1
fi

diff test.output2 ${SCRIPT_DIR}/test.golden2
if [ $? -ne "0" ]
then
  echo "FAIL - golden file differs, exiting"
  exit 1
fi

echo "SUCCESS"
