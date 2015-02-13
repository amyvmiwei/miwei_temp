#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
SPEC_FILE="$SCRIPT_DIR/../../data/random-test.spec"
MAX_KEYS=50000

$HT_HOME/bin/ht-start-test-servers.sh --clear

$HT_HOME/bin/ht hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

echo "================="
echo "random WRITE test"
echo "================="
$HT_HOME/bin/ht load_generator update --spec-file=$SPEC_FILE \
        --max-keys=$MAX_KEYS --rowkey-seed=42

sleep 5

echo "================="
echo "random READ test"
echo "================="
$HT_HOME/bin/ht load_generator query --spec-file=$SPEC_FILE \
        --max-keys=$MAX_KEYS --rowkey-seed=42

pushd .
cd $HT_HOME
./bin/ht-start-test-servers.sh --clear
popd

$HT_HOME/bin/ht hypertable --no-prompt < $SCRIPT_DIR/create-table-memory.hql

echo "============================="
echo "random WRITE test (IN_MEMORY)"
echo "============================="
$HT_HOME/bin/ht load_generator update --spec-file=$SPEC_FILE \
        --max-keys=$MAX_KEYS --rowkey-seed=42

sleep 5

echo "============================"
echo "random READ test (IN_MEMORY)"
echo "============================"
$HT_HOME/bin/ht load_generator query --spec-file=$SPEC_FILE \
        --max-keys=$MAX_KEYS --rowkey-seed=42

