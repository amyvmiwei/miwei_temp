#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #890"
echo "======================="

echo "compiling"
JARS=`perl $SCRIPT_DIR/jars.pl $HT_HOME`
javac -classpath "$JARS:." $SCRIPT_DIR/TestSerializers.java

echo "running"
java -ea -classpath $HT_HOME/lib/java/*:$SCRIPT_DIR:. TestSerializers
