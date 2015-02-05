#!/usr/bin/env bash

SCRIPT_DIR=`dirname $0`
HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
RS_PIDFILE=$HT_HOME/run/RangeServer.pid

set -v

$HT_HOME/bin/start-test-servers.sh --no-thriftbroker --no-rangeserver --clean

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS_PIDFILE \
    --induce-failure="create-scanner-user-1:exit:0"  2>&1 > rangeserver.output&

cat $SCRIPT_DIR/create-table.hql | $HT_HOME/bin/ht hypertable --batch

$HT_HOME/bin/ht ht_load_generator update --spec-file=${SCRIPT_DIR}/data.spec \
    --table ScannerTimeoutTest --max-bytes=5M

echo "use '/'; select * from ScannerTimeoutTest KEYS_ONLY;" | $HT_HOME/bin/ht hypertable --batch --Hypertable.Request.Timeout=15000

$HT_HOME/bin/stop-servers.sh

exit 0
