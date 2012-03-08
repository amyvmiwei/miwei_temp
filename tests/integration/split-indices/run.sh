#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME;
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
    --Hypertable.RangeServer.Range.SplitSize=20K \
    --Hypertable.RangeServer.Scanner.BufferSize=1K

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

echo "======================="
echo "Split indices tests"
echo "======================="
$HT_HOME/bin/ht ht_load_generator update --spec-file=${SCRIPT_DIR}/data.spec \
    --table=IndexTest --max-keys=10000 \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=20K \
    --Hypertable.Mutator.FlushDelay=5 \
    2>&1

sleep 1
$HT_HOME/bin/start-test-servers.sh --no-thriftbroker 

cat ${SCRIPT_DIR}/test-indices.hql | $HT_HOME/bin/ht hypertable \
    --no-prompt --test-mode > test-indices.output
diff test-indices.output ${SCRIPT_DIR}/test-indices.golden 
if [ $? -ne "0" ]
then
  echo "FAIL - indices tests differ, exiting"
  exit 1
fi

echo "SUCCESS"
