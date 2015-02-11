#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #890"
echo "======================="

# make sure hypertable jar files are copied into lib/java
$HT_HOME/bin/ht-set-hadoop-distro.sh cdh5

cp $SCRIPT_DIR/TestSerializers.java .

echo "compiling"
JARS=$HT_HOME/lib/java/libthrift.jar:$HT_HOME/lib/java/hypertable.jar:$SCRIPT_DIR:.
javac -classpath $JARS ./TestSerializers.java

echo "running"
java -ea -classpath $JARS TestSerializers
