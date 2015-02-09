#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME
SCRIPT_DIR=`dirname $0`
SPEC_FILE=$SCRIPT_DIR/data.spec
MAX_KEYS=${MAX_KEYS:-"5000"}
AG_MAXMEM=250000

. $HT_HOME/bin/ht-env.sh

restart_servers() {
  $HT_HOME/bin/ht-start-test-servers.sh --clear --no-thriftbroker \
      --Hypertable.RangeServer.AccessGroup.MaxMemory=$AG_MAXMEM \
      --Hypertable.Mutator.FlushDelay=10
}

restart_servers_noclean() {
  $HT_HOME/bin/ht-start-test-servers.sh --no-thriftbroker \
      --Hypertable.RangeServer.AccessGroup.MaxMemory=$AG_MAXMEM \
      --Hypertable.Mutator.FlushDelay=10
}

test() {
  restart_servers

  filter_type=$1
  echo "Run test for Bloom filter of type ${filter_type}"

  $HT_HOME/bin/ht shell --no-prompt < \
      $SCRIPT_DIR/create-bloom-${filter_type}-table.hql

  echo "================="
  echo "random WRITE test"
  echo "================="
  $HT_HOME/bin/ht load_generator update --spec-file=$SPEC_FILE \
      --max-keys=$MAX_KEYS --rowkey-seed=68

  echo "================="
  echo "random READ test"
  echo "================="
  $HT_HOME/bin/ht load_generator query --spec-file=$SPEC_FILE \
      --max-keys=$MAX_KEYS --rowkey-seed=68

  restart_servers_noclean
  echo "================="
  echo "random READ test"
  echo "================="
  $HT_HOME/bin/ht load_generator query --spec-file=$SPEC_FILE \
      --max-keys=$MAX_KEYS --rowkey-seed=68
}

test $1
