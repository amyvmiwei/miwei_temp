#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #833"
echo "======================="

java -ea -classpath $HT_HOME/lib/java/commons-cli-1.2.jar:$HT_HOME/lib/java/commons-httpclient-3.1.jar:$HT_HOME/lib/java/commons-logging-1.0.4.jar:$HT_HOME/lib/java/guava-r09-jarjar.jar:$HT_HOME/lib/java/hadoop-0.20.2-cdh3u3-core.jar:$HT_HOME/lib/java/hbase-0.90.4-cdh3u2.jar:$HT_HOME/lib/java/hive-exec-0.7.0-cdh3u0.jar:$HT_HOME/lib/java/hive-exec-0.7.1-cdh3u3.jar:$HT_HOME/lib/java/hive-metastore-0.7.0-cdh3u0.jar:$HT_HOME/lib/java/hive-metastore-0.7.1-cdh3u3.jar:$HT_HOME/lib/java/hive-serde-0.7.0-cdh3u0.jar:$HT_HOME/lib/java/hive-serde-0.7.1-cdh3u3.jar:$HT_HOME/lib/java/hypertable-0.9.5.6-examples.jar:$HT_HOME/lib/java/hypertable-0.9.5.6.jar:$HT_HOME/lib/java/junit-4.3.1.jar:$HT_HOME/lib/java/libthrift-0.8.0.jar:$HT_HOME/lib/java/log4j-1.2.13.jar:$HT_HOME/lib/java/slf4j-api-1.5.8.jar:$HT_HOME/lib/java/slf4j-log4j12-1.5.8.jar:$HT_HOME/lib/java/zookeeper-3.3.3-cdh3u2.jar org.hypertable.hadoop.mapreduce.ScanSpec
