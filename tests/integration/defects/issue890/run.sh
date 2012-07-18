#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #890"
echo "======================="

echo "compiling"
javac -classpath $HT_HOME/lib/java/hypertable-0.9.5.6.jar $SCRIPT_DIR/TestSerializers.java

echo "running"
java -ea -classpath $HT_HOME/lib/java/*:$SCRIPT_DIR:. TestSerializers
