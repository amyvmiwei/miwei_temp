#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #833"
echo "======================="

# make sure hypertable jar files are copied into lib/java
$HT_HOME/bin/ht-set-hadoop-distro.sh cdh5

JARS=$HT_HOME/lib/java/*:.
java -ea -cp $JARS org.hypertable.hadoop.mapreduce.ScanSpec
