#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/0.9.6.0"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
    --Hypertable.RangeServer.Range.SplitSize=20K \
    --Hypertable.RangeServer.Scanner.BufferSize=1K

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

echo "======================="
echo "Split offset tests"
echo "======================="
$HT_HOME/bin/ht ht_load_generator update --spec-file=${SCRIPT_DIR}/data.spec \
    --table=OffsetTest --max-keys=50000 \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=20K \
    --Hypertable.Mutator.FlushDelay=5 \
    2>&1

sleep 1

# now test the offset
cat ${SCRIPT_DIR}/test-offsets.hql | $HT_HOME/bin/ht hypertable \
    --no-prompt --test-mode > test-offsets.output
diff test-offsets.output ${SCRIPT_DIR}/test-offsets.golden
if [ $? -ne "0" ]
then
  echo "FAIL - offset tests differ, exiting"
  exit 1
fi

echo "SUCCESS"
