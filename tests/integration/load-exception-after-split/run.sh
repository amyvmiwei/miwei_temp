#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
SCRIPT_DIR=`dirname $0`
SPEC_FILE="$SCRIPT_DIR/../../data/random-test.spec"
MAX_KEYS=${MAX_KEYS:-"25000"}

. $HT_HOME/bin/ht-env.sh

$HT_HOME/bin/ht-start-test-servers.sh --clear --no-thriftbroker \
    --Hypertable.RangeServer.Range.SplitSize=2500K \
    --induce-failure=load-range-1:throw:3

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

echo "=== Writing $MAX_KEYS cells ==="
$HT_HOME/bin/ht load_generator update --spec-file=$SPEC_FILE \
        --max-keys=$MAX_KEYS --rowkey-seed=75

sleep 5

echo "=== Reading $MAX_KEYS cells ==="
$HT_HOME/bin/ht load_generator query --spec-file=$SPEC_FILE \
        --max-keys=$MAX_KEYS --rowkey-seed=75

