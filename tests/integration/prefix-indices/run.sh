#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME;
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

echo "======================="
echo "Prefix indices tests"
echo "======================="
$HT_HOME/bin/ht ht_load_generator update --spec-file=${SCRIPT_DIR}/data.spec \
    --table=IndexTest --max-keys=200000 \
    2>&1

# this query uses the index
$HT_HOME/bin/ht shell --test-mode --batch --no-prompt --namespace / --exec "SELECT Field1 FROM IndexTest WHERE Field1 =^ '';" > test-indices.output1
# this query does NOT use the index
$HT_HOME/bin/ht shell --test-mode --batch --no-prompt --namespace / --exec "SELECT Field1 FROM IndexTest;" > test-indices.output2

# then simply compare both
diff test-indices.output1 test-indices.output2
if [ $? -ne "0" ]
then
  echo "FAIL - indice tests differ, exiting"
  exit 1
fi

echo "SUCCESS"
