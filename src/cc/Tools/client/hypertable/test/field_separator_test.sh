#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

cat $SCRIPT_DIR/field_separator_test.hql | $HT_HOME/bin/ht shell --batch

diff field_separator_test.tsv field_separator_test_output.tsv

