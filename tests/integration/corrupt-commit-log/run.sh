#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME
SCRIPT_DIR=`dirname $0`
WRITE_TOTAL=${WRITE_TOTAL:-"25000000"}

. $HT_HOME/bin/ht-env.sh

$HT_HOME/bin/ht-start-test-servers.sh --clear --no-thriftbroker

$HT_HOME/bin/ht shell --test-mode < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht load_generator update --rowkey.component.0.type=integer --rowkey.component.0.order=random --rowkey.component.0.format="%030lld" --Field.value.size=1000 --row-seed=1 --max-bytes=$WRITE_TOTAL

echo "wait for maintenance; quit;" | $HT_HOME/bin/ht rangeserver --batch

\rm -f /tmp/0.good /tmp/0

echo "copyToLocal /hypertable/servers/rs1/log/user/0 /tmp/0.good;" | $HT_HOME/bin/ht fsbroker --batch

#
# Corrupt last block test
#
LAST_BLOCK=`strings -a -t d /tmp/0.good | fgrep COMMITDATA | tail -1`
BASE_OFFSET=`echo $LAST_BLOCK | cut -f1 -d' '`
STR=`echo $LAST_BLOCK | cut -f2 -d' '`
let OFFSET=BASE_OFFSET+${#STR}+4

dd bs=$OFFSET if=/tmp/0.good of=/tmp/0 count=1
echo "a" >> /tmp/0
let SKIP_OFFSET=OFFSET+1
dd bs=$SKIP_OFFSET if=/tmp/0.good of=/tmp/0 skip=1 seek=1

$HT_HOME/bin/ht-stop-servers.sh
$HT_HOME/bin/ht-start-fsbroker.sh local

echo "rm /hypertable/servers/rs1/log/user/0;" | $HT_HOME/bin/ht fsbroker --batch
echo "copyFromLocal /tmp/0 /hypertable/servers/rs1/log/user/0;" | $HT_HOME/bin/ht fsbroker --batch

$HT_HOME/bin/ht-start-test-servers.sh --no-thriftbroker
$HT_HOME/bin/ht-check-rangeserver.sh
if [ $? -ne 1 ]; then
  echo "Error - RangeServer not reporting WARNING as it should"
  exit 1
fi

#
# Last block header truncated
#
$HT_HOME/bin/ht-stop-rangeserver.sh
\rm -f /tmp/0
dd bs=$OFFSET if=/tmp/0.good of=/tmp/0 count=1
echo "rm /hypertable/servers/rs1/log/user/0;" | $HT_HOME/bin/ht fsbroker --batch
echo "copyFromLocal /tmp/0 /hypertable/servers/rs1/log/user/0;" | $HT_HOME/bin/ht fsbroker --batch
$HT_HOME/bin/ht-start-rangeserver.sh
$HT_HOME/bin/ht-check-rangeserver.sh
if [ $? -ne 1 ]; then
  echo "Error - RangeServer not reporting WARNING as it should"
  exit 1
fi

#
# Last block payload truncated
#
$HT_HOME/bin/ht-stop-rangeserver.sh
\rm -f /tmp/0
let TRUNCATE_OFFSET=OFFSET+50
dd bs=$TRUNCATE_OFFSET if=/tmp/0.good of=/tmp/0 count=1
echo "rm /hypertable/servers/rs1/log/user/0;" | $HT_HOME/bin/ht fsbroker --batch
echo "copyFromLocal /tmp/0 /hypertable/servers/rs1/log/user/0;" | $HT_HOME/bin/ht fsbroker --batch
$HT_HOME/bin/ht-start-rangeserver.sh
$HT_HOME/bin/ht-check-rangeserver.sh
if [ $? -ne 1 ]; then
  echo "Error - RangeServer not reporting WARNING as it should"
  exit 1
fi

WARNING_COUNT=`grep WARNING $HT_HOME/run/STATUS.htRangeServer | wc -l`
if [ $WARNING_COUNT -ne 3 ]; then
  cat $HT_HOME/run/STATUS.htRangeServer
  echo "ERROR: Expected 3 warnings, only got $WARNING_COUNT"
  exit 1
fi

# Cleanup
$HT_HOME/bin/ht-destroy-database.sh
\rm -f /tmp/0.good /tmp/0

exit 0