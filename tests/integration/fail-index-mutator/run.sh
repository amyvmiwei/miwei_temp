#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME;
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker                \
                --induce-failure=fail-index-mutator-0:signal:0

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

echo "======================="
echo "Fail index mutator"
echo "======================="
$HT_HOME/bin/ht ht_load_generator update --spec-file=${SCRIPT_DIR}/data.spec \
    --table=IndexTest --max-keys=10000 2>&1 > inducer.output

grep "HYPERTABLE induced failure" inducer.output
if [ $? -ne "0" ]
then
    echo "Inducer didn't fire"
    exit -1
fi

$HT_HOME/bin/ht shell --namespace / --test-mode --no-prompt \
    --exec "SELECT Field1 FROM IndexTest INTO FILE 'Field1.output';"
$HT_HOME/bin/ht shell --namespace / --test-mode --no-prompt \
    --exec "SELECT v1 FROM '^IndexTest' INTO FILE 'Field1-index.output';"

diff Field1.output $SCRIPT_DIR/Field1.golden
if [ $? -ne "0" ]
then
    echo "Field1.output contains bad data"
    exit -1
fi

diff Field1-index.output $SCRIPT_DIR/Field1-index.golden
if [ $? -ne "0" ]
then
    echo "Field1-index.output contains bad data"
    exit -1
fi

echo "SUCCESS"

