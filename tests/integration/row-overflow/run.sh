#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"20000000"}
DISABLE_ARG=

check_exit_status() {
    if [ $? -ne 0 ]; then
	if test -z "${DISABLE_ARG}" ; then
	    echo "($1) 'row overflow' not found, exiting"
	    exit -1
	fi
    else
	if test -n "${DISABLE_ARG}" ; then
	    echo "($1) 'row overflow' found, exiting"
	    exit -1
	fi
    fi
}

if [ $# == 1 ]; then
	if [ "$1" == "--disabled" ]; then
	    DISABLE_ARG="--Hypertable.RangeServer.Range.RowSize.Unlimited=true"
	    echo "Running test with --Hypertable.RangeServer.Range.RowSize.Unlimited=true"
	else
	    echo "Invalid argument: $1"
	    exit 1
	fi
fi


echo "======================================"
echo "Row overflow WRITE test (TableMutator)"
echo "======================================"
$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
    --Hypertable.RangeServer.Range.SplitSize=2000000 $DISABLE_ARG
$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql
$HT_HOME/bin/ht ht_write_test \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=50K \
    --Hypertable.Mutator.FlushDelay=5 --max-keys=1 \
    $DATA_SIZE 2>&1 | grep -c '^Failed.*row overflow'

check_exit_status 1

echo "======================================"
echo "Row overflow WRITE test (HQL Insert)"
echo "======================================"
$HT_HOME/bin/start-test-servers.sh --clear \
    --Hypertable.RangeServer.Range.SplitSize=200000 $DISABLE_ARG
$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql
$HT_HOME/bin/ht shell --no-prompt --batch \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=50K \
    --Hypertable.Mutator.FlushDelay=5 < $SCRIPT_DIR/row-overflow.hql \
    2>&1 | grep -c '^Failed.*row overflow'
check_exit_status 2

echo "======================================"
echo "Row overflow WRITE test (Thrift mutator)"
echo "======================================"
# insert one more row via thrift
$SCRIPT_DIR/thrift_insert.py RandomTest thisisarowkey Field 2>&1 | grep -c 'row overflow'
check_exit_status 3

exit 0
