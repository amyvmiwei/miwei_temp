#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #910"
echo "======================="

# delete old monitoring data
\rm -rf $HT_HOME/run/monitoring
\rm -rf $HT_HOME/log/*

# start the cluster
$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker
sleep 5

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql
$HT_HOME/bin/ht ht_load_generator update --spec-file=${SCRIPT_DIR}/data.spec \
        --table=LoadTest --max-keys=10000000 2>&1

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/test.hql > test.output

diff test.output $SCRIPT_DIR/test.golden
if [ $? -ne 0 ];
then
  echo "query differs; exiting"
  exit -1
fi

echo "SUCCESS"
exit 0
