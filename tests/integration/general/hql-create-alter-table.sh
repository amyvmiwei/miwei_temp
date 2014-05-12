#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker

cat $SCRIPT_DIR/hql-create-alter-table.hql | $HT_HOME/bin/ht shell --test-mode >& hql-create-alter-table.output

diff $SCRIPT_DIR/hql-create-alter-table.golden hql-create-alter-table.output
if [ $? -ne 0 ]; then
    $HT_HOME/bin/stop-servers.sh
    echo "error: diff returned non-zero, exiting..."
    exit 1
fi

$HT_HOME/bin/stop-servers.sh
exit 0

