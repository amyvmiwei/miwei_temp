#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #833"
echo "======================="

# make sure hypertable jar files are copied into lib/java
$HT_HOME/bin/set-hadoop-distro.sh cdh3

java -ea -cp $HT_HOME/lib/java/*:. org.hypertable.hadoop.mapreduce.ScanSpec
