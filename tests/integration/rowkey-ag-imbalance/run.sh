#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"20000000"}

$HT_HOME/bin/start-test-servers.sh --clear --no-rangeserver --no-thriftbroker \
    --Hypertable.RangeServer.Range.SplitSize=2000000 \
    --Hypertable.Master.Split.SoftLimitEnabled=false

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.RangeServer.Maintenance.Interval=100 > rangeserver.output 2>&1 &

sleep 2

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht ht_load_generator update --spec-file=foo.spec --max-bytes=2200000

sleep 2

$HT_HOME/bin/ht ht_load_generator update --spec-file=bar.spec --max-bytes=10000000 \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=200K --Hypertable.Mutator.FlushDelay=100

sleep 1

fgrep ERROR rangeserver.output

if [ $? == 0 ] ; then
    $HT_HOME/bin/stop-servers.sh
    cp -r $HT_HOME/fs .
    exit 1
fi

$HT_HOME/bin/stop-servers.sh

exit 0
