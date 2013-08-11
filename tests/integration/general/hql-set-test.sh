#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker

echo "SET READONLY=true; CREATE TABLE SystemState (col);" | $HT_HOME/bin/ht shell --batch

echo "INSERT INTO SystemState VALUES (\"row\", \"col\", \"value\");" \
    | $HT_HOME/bin/ht shell --batch >& hql-set-test-a.output

grep -i readonly hql-set-test-a.output

if [ $? != 0 ]; then
    echo "error: System not put into READONLY mode as it should have"
    exit 1
fi

echo "SET READONLY=false;" | $HT_HOME/bin/ht shell --batch
echo "INSERT INTO SystemState VALUES (\"row\", \"col\", \"value\");" \
    | $HT_HOME/bin/ht shell --batch
echo "SELECT * FROM SystemState;" | $HT_HOME/bin/ht shell --batch > hql-set-test-b.output

diff hql-set-test-b.output $SCRIPT_DIR/hql-set-test-b.golden

if [ $? != 0 ]; then
    echo "error: Table dump incorrect after disabling READONLY mode"
    exit 1
fi

exit 0
