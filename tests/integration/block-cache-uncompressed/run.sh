#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
WRITE_SIZE=${WRITE_SIZE:-"20000000"}
RS_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid

$HT_HOME/bin/start-test-servers.sh --clear --no-rangeserver

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS_PIDFILE \
   --Hypertable.RangeServer.BlockCache.Compressed=false \
   --Hypertable.RangeServer.Maintenance.Interval=100 \
   --Hypertable.RangeServer.Range.SplitSize=400K 2>1 > rangeserver.output&

sleep 3

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht ht_load_generator update \
    --Hypertable.Mutator.FlushDelay=50 \
    --rowkey.component.0.order=random \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.min=0 \
    --rowkey.component.0.max=1000000 \
    --row-seed=1 \
    --Field.value.size=1000 \
    --max-bytes=$WRITE_SIZE

sleep 5

$HT_HOME/bin/ht ht_load_generator query \
    --rowkey.component.0.order=random \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.min=0 \
    --rowkey.component.0.max=1000000 \
    --row-seed=3 \
    --Field.value.size=1000 \
    --max-keys=1000

if [ $? != 0 ] ; then
  exit 1
fi

kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.pid`
\rm -f $HT_HOME/run/Hypertable.RangeServer.pid
$HT_HOME/bin/clean-database.sh

exit 0


