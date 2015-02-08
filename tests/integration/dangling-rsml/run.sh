#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME
SCRIPT_DIR=`dirname $0`
RET=0

. $HT_HOME/bin/ht-env.sh

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
   --Hypertable.RangeServer.Range.SplitSize=1M

$HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/stop-servers.sh

$HT_HOME/bin/start-test-servers.sh --no-thriftbroker

echo "drop table if exists LoadTest;" | $HT_HOME/bin/ht shell --batch

$HT_HOME/bin/stop-servers.sh

# Copy previous RSML file which contains the deleted table LoadTest
\cp -f $HT_HOME/fs/local/hypertable/servers/rs1/log/rsml/0 $HT_HOME/fs/local/hypertable/servers/rs1/log/rsml/1
\cp -f $HT_HOME/fs/local/hypertable/servers/rs1/log/rsml/0 $HT_HOME/run/log_backup/rsml/rs1/1

$HT_HOME/bin/start-test-servers.sh --no-thriftbroker

$HT_HOME/bin/ht-check-rangeserver.sh
if [ $? -ne 0 ]; then
  RET=1
fi

$HT_HOME/bin/stop-servers.sh

exit $RET


