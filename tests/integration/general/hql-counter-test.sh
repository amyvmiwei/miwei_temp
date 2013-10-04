#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
    --Hypertable.RangeServer.Maintenance.Interval=500

cat $SCRIPT_DIR/hql-counter-test.hql | $HT_HOME/bin/ht shell --batch
if [ $? -ne 0 ]; then
    $HT_HOME/bin/stop-servers.sh
    echo "Hypertable shell failure!"
    exit 1
fi

diff hql-counter-test.output.1 $SCRIPT_DIR/hql-counter-test.golden
if [ $? -ne 0 ]; then
    $HT_HOME/bin/stop-servers.sh
    echo "Diff encountered!"
    exit 1
fi

diff hql-counter-test.output.2 $SCRIPT_DIR/hql-counter-test.golden
if [ $? -ne 0 ]; then
    $HT_HOME/bin/stop-servers.sh
    echo "Diff encountered!"
    exit 1
fi

$HT_HOME/bin/stop-servers.sh
exit 0

