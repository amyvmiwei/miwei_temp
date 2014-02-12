#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
DATA_SEED=42
DATA_SIZE=${DATA_SIZE:-"2000000"}
DIGEST="openssl dgst -md5"
RUN_DIR=`pwd`

. $HT_HOME/bin/ht-env.sh

# get rid of all old logfiles
\rm -rf $HT_HOME/log/*

# shut down range servers
kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.*.pid`
\rm -f $HT_HOME/run/Hypertable.RangeServer.*.pid

# maybe generate test data
if [ ! -s golden_dump.md5 ] ; then
    $HT_HOME/bin/ht load_generator --spec-file=$SCRIPT_DIR/data.spec \
        --max-keys=$DATA_SIZE --row-seed=$DATA_SEED \
        --stdout update | cut -f1 | tail -n +2 | sort -u > golden_dump.txt
    $DIGEST < golden_dump.txt > golden_dump.md5
fi

# Start servers (minus range server)
$HT_HOME/bin/start-test-servers.sh --no-rangeserver \
    --no-thriftbroker --clear --induce-failure="bad-rsml:throw:0" \
    --config=${SCRIPT_DIR}/test.cfg 

# Start rs1
$HT_HOME/bin/ht Hypertable.RangeServer \
    --config=${SCRIPT_DIR}/test.cfg \
    --verbose \
    --pidfile=$HT_HOME/run/Hypertable.RangeServer.rs1.pid \
    --Hypertable.RangeServer.ProxyName=rs1 \
    --Hypertable.RangeServer.Port=15870 > rangeserver.rs1.output &

# Start rs2
$HT_HOME/bin/ht Hypertable.RangeServer \
    --config=${SCRIPT_DIR}/test.cfg \
    --verbose \
    --pidfile=$HT_HOME/run/Hypertable.RangeServer.rs2.pid \
    --Hypertable.RangeServer.ProxyName=rs2 \
    --Hypertable.RangeServer.Port=15871 > rangeserver.rs2.output &

# Create test table
$HT_SHELL --batch < $SCRIPT_DIR/create-test-table.hql
if [ $? != 0 ] ; then
    echo "Unable to create table 'BadRsmlTest', exiting ..."
    exit 1
fi

# Load data
$HT_HOME/bin/ht load_generator --spec-file=$SCRIPT_DIR/data.spec \
    --max-keys=$DATA_SIZE --row-seed=$DATA_SEED --table=BadRsmlTest \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=100K \
    update
if [ $? != 0 ] ; then
    echo "Problem loading table 'BadRsmlTest', exiting ..."
    exit 1
fi

## Kill rs2
kill `cat $HT_HOME/run/Hypertable.RangeServer.rs2.pid`

## Wait for "Problem reading RSML" to appear in Master log
grep "Problem reading RSML" $HT_HOME/log/Hypertable.Master.log
while [ $? -ne 0 ]; do
    echo "Waiting for \"Problem reading RSML\" to appear in master log ..."
    sleep 5
    grep "Problem reading RSML" $HT_HOME/log/Hypertable.Master.log    
done

## Restart Master
kill `cat $HT_HOME/run/Hypertable.Master.pid`
$HT_HOME/bin/start-master.sh --config=${SCRIPT_DIR}/test.cfg

# dump keys
$HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
    | grep -v "hypertable" | grep -v "Waiting for connection to Hyperspace" > dbdump.output
if [ $? != 0 ] ; then
    echo "Problem dumping table 'failover-test', exiting ..."
    exit 1
fi

$DIGEST < dbdump.output > dbdump.md5
diff golden_dump.md5 dbdump.md5 > out
if [ $? != 0 ] ; then
    echo "Test $TEST FAILED." >> report.txt
    echo "Test $TEST FAILED." >> errors.txt
    cat out >> report.txt
    touch error
    $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
        | grep -v "hypertable" | grep -v "Waiting for connection to Hyperspace" > dbdump.output.again
    exec 1>&-
    sleep 86400
else
    echo "Test $TEST PASSED." >> report.txt
fi

# Shut down range servers
kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.*.pid`
\rm -f $HT_HOME/run/Hypertable.RangeServer.*.pid

# Shut down remaining servers
$HT_HOME/bin/stop-servers.sh

exit 0
