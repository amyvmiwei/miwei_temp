#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #833"
echo "======================="

java -ea -cp $HT_HOME/lib/java/*:. org.hypertable.hadoop.mapreduce.ScanSpec
