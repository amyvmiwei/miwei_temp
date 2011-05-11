#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"100000000"}
THREADS=${THREADS:-"8"}
ITERATIONS=${ITERATIONS:-"1"}
TESTNUM=${TESTNUM:-"4"}

if [ ! -e Test1-data.txt ] ; then
  gzip -d Test1-data.txt.gz
fi
if [ ! -e Test2-data.txt ] ; then
  gzip -d Test2-data.txt.gz
fi
if [ ! -e Test4-data.txt ] ; then
  gzip -d Test4-data.txt.gz
fi

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
    --Hypertable.RangeServer.CellStore.TargetSize.Minimum=1500K \
    --Hypertable.RangeServer.Maintenance.MergingCompaction.Delay=0 \
    --Hypertable.RangeServer.Maintenance.Interval=100 \
    --Hypertable.RangeServer.Timer.Interval=100 \
    --Hypertable.RangeServer.AccessGroup.MaxMemory=1M

echo "create namespace '/test';" | $HT_HOME/bin/ht shell --test-mode

for ((i=0; i<$ITERATIONS; i++)) ; do

    $HT_HOME/bin/ht shell --test-mode < ${SOURCE_DIR}/initialize.hql > init.out
    if [ $? != 0 ] ; then
        echo "Iteration ${i} of rsTest failed, exiting..."
        exit 1
    fi

    for ((j=1; j<=$TESTNUM; j++)) ; do

        $HT_HOME/bin/ht ht_rsclient --test-mode --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=550K \
            localhost < ${SOURCE_DIR}/Test${j}.cmd > Test${j}.output
        if [ $? != 0 ] ; then
            echo "Iteration ${i} of rsTest failed, exiting..."
            exit 1
        fi

        diff Test${j}.output ${SOURCE_DIR}/Test${j}.golden
        if [ $? != 0 ] ; then
            echo "Iteration ${i} of rsTest failed, exiting..."
            exit 1
        fi
    done
done

exit 0
