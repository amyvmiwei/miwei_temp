#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME;
SCRIPT_DIR=`dirname $0`

function finish {
    $HT_HOME/bin/ht-destroy-database.sh
}
trap finish EXIT

run_test() {

    local max_keys=$1
    local limit=$2

    $HT_HOME/bin/ht load_generator update --spec-file=${SCRIPT_DIR}/data-a.spec \
        --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=40K \
        --Hypertable.Mutator.FlushDelay=5 \
        --max-keys=$max_keys
    if [ $? -ne 0 ]; then
        echo "Load generator failure"
        exit 1
    fi

    sleep 5

    $HT_HOME/bin/ht load_generator update --spec-file=${SCRIPT_DIR}/data-b.spec \
        --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=40K \
        --Hypertable.Mutator.FlushDelay=5 \
        --max-keys=$max_keys
    if [ $? -ne 0 ]; then
        echo "Load generator failure"
        exit 1
    fi

    sleep 5

    $HT_HOME/bin/ht load_generator update --spec-file=${SCRIPT_DIR}/data-c.spec \
        --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=40K \
        --Hypertable.Mutator.FlushDelay=5 \
        --max-keys=$max_keys
    if [ $? -ne 0 ]; then
        echo "Load generator failure"
        exit 1
    fi

    sleep 5

    $HT_HOME/bin/ht load_generator update --spec-file=${SCRIPT_DIR}/data-a.spec \
        --delete-percentage=100 --max-keys=$max_keys
    if [ $? -ne 0 ]; then
        echo "Load generator failure (deletes)"
        exit 1
    fi

    $HT_HOME/bin/ht load_generator update --spec-file=${SCRIPT_DIR}/data-b.spec \
        --delete-percentage=100 --max-keys=$max_keys
    if [ $? -ne 0 ]; then
        echo "Load generator failure (deletes)"
        exit 1
    fi


    $SCRIPT_DIR/verify_limit_query.py $max_keys $limit

}

#
# NO ACCESS GROUP DELETE test
#

$HT_HOME/bin/ht-start-test-servers.sh --clear \
    --Hypertable.RangeServer.Range.SplitSize=20K \
    --Hypertable.Master.Split.SoftLimitEnabled=false \
    --Hypertable.RangeServer.Scanner.BufferSize=1K \
    --Hypertable.RangeServer.Maintenance.Interval=5000

$HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/create-table.hql

run_test 5000 1
if [ $? -ne 0 ]; then
  echo "NO ACCESS GROUP DELETE test failed"
  exit 1
fi

#
# ACCESS GROUP DELETE test
#

$HT_HOME/bin/ht-start-test-servers.sh --clear \
    --Hypertable.RangeServer.Range.SplitSize=20K \
    --Hypertable.Master.Split.SoftLimitEnabled=false \
    --Hypertable.RangeServer.Scanner.BufferSize=1K \
    --Hypertable.RangeServer.Maintenance.Interval=5000

$HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/create-table-with-ags.hql

run_test 5000 1
if [ $? -ne 0 ]; then
  echo "ACCESS GROUP DELETE test failed"
  exit 1
fi


#
# EMPTY RANGE test
#

$HT_HOME/bin/ht-start-test-servers.sh --clear \
    --Hypertable.RangeServer.Range.SplitSize=20K \
    --Hypertable.Master.Split.SoftLimitEnabled=false \
    --Hypertable.RangeServer.Scanner.BufferSize=1K \
    --Hypertable.RangeServer.Maintenance.Interval=2000

$HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht load_generator update --spec-file=${SCRIPT_DIR}/data-a.spec \
        --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=30K \
        --Hypertable.Mutator.FlushDelay=5 \
        --max-keys=2000

sleep 5

echo "compact table LoadTest;" | $HT_HOME/bin/ht shell --batch

echo "wait for maintenance;" | $HT_HOME/bin/ht rangeserver --batch

$HT_HOME/bin/ht load_generator update --spec-file=${SCRIPT_DIR}/data-a.spec \
    --delete-percentage=100 --max-keys=1000
if [ $? -ne 0 ]; then
  echo "Failure loading deletes for EMPTY RANGE test"
  exit 1
fi

$SCRIPT_DIR/verify_limit_query.py 2000 1
if [ $? -ne 0 ]; then
  echo "EMPTY RANGE test failed"
  exit 1
fi
