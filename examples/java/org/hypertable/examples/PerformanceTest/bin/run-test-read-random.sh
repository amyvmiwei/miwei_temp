#!/usr/bin/env bash

#. ~/.ssh-agent

export BINDIR=$(cd `dirname "$0"` && pwd)
. $BINDIR/test-config.sh

usage() {
  echo ""
  echo "usage: run-test-read-random.sh <system> <key-size> <value-size>"
  echo ""
  echo "This script is used to run a performance benchmark.  The <system>"
  echo "argument indicates which system to run the test against and"
  echo "can take a value of 'hypertable' or 'hbase'"
  echo ""
}

if [ "$#" -ne 3 ]; then
  usage
  exit 1
fi

SYSTEM=$1
shift
KEY_SIZE=$1
shift
VALUE_SIZE=$1
shift

if [ "$SYSTEM" != "hypertable" ] && [ "$SYSTEM" != "hbase" ] ; then
    echo "ERROR:  Unrecognized system name '$SYSTEM'"
    exit 1
fi

let CELLSIZE=KEY_SIZE+VALUE_SIZE
let KEYMAX=DATA_SIZE/CELLSIZE
let DISTRANGE=100000000
let KEYCOUNT=5000000

cap stop_test
cap stop
cap push_config
cap start
sleep 180

cap -S test_driver=$SYSTEM -S client_multiplier=12 -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR --random --key-max=$KEYMAX --submit-exactly=$KEYCOUNT read $KEY_SIZE $VALUE_SIZE $DATA_SIZE" run_test 

cap stop_test
cap stop
cap -S config=$PWD/perftest-hypertable-query.cfg push_config
cap -S config=$PWD/perftest-hypertable-query.cfg start
sleep 180

cap -S test_driver=$SYSTEM -S client_multiplier=12 -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR --distribution-range=$DISTRANGE --random --zipf --key-max=$KEYMAX --submit-exactly=$KEYCOUNT read $KEY_SIZE $VALUE_SIZE $DATA_SIZE" run_test 

