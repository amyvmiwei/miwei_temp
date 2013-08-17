#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

cat $SCRIPT_DIR/delete_test.hql | $HT_HOME/bin/ht shell --batch

diff delete_test_output.tsv $SCRIPT_DIR/delete_test_golden.tsv

