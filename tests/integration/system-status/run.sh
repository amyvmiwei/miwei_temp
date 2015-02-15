#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

function on_exit() {
    $HT_HOME/bin/ht-destroy-database.sh
}

trap on_exit EXIT

\rm -f output
$HT_HOME/bin/ht-destroy-database.sh

$HT_HOME/bin/ht-check.sh >> output
if [ $? -ne 2 ]; then
  echo "(1) Status should be CRITICAL"
  exit 1
fi

$HT_HOME/bin/ht-start-hyperspace.sh
$HT_HOME/bin/ht-check.sh >> output
if [ $? -ne 2 ]; then
  echo "(2) Status is $? when it should be CRITICAL"
  exit 1
fi

$HT_HOME/bin/ht-start-fsbroker.sh local
$HT_HOME/bin/ht-start-master.sh
$HT_HOME/bin/ht-check.sh >> output
if [ $? -ne 2 ]; then
  echo "(3) Status is $? when it should be CRITICAL"
  exit 1
fi

$HT_HOME/bin/ht-start-rangeserver.sh
$HT_HOME/bin/ht-check.sh >> output
if [ $? -ne 0 ]; then
  echo "(4) Status is $? when it should be OK"
  exit 1
fi


kill -STOP `cat $HT_HOME/run/RangeServer.pid`
$HT_HOME/bin/ht-check.sh -t 7 >> output
if [ $? -ne 1 ]; then
  echo "(5) Status is $? when it should be WARNING"
  exit 1
fi
kill -CONT `cat $HT_HOME/run/RangeServer.pid`

# NOTE: The output line of the next two tests are unstable which
#       is why we're not capturing them in the output file

$HT_HOME/bin/ht-stop-rangeserver.sh
$HT_HOME/bin/ht-check.sh
if [ $? -ne 2 ]; then
  echo "(6) Status is $? when it should be CRITICAL"
  exit 1
fi

$HT_HOME/bin/ht-start-rangeserver.sh
$HT_HOME/bin/ht-stop-fsbroker.sh
$HT_HOME/bin/ht-check.sh
if [ $? -ne 2 ]; then
  echo "(7) Status is $? when it should be CRITICAL"
  exit 1
fi

cat output | sed 's/\([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\)//g' > output.stripped

diff output.stripped $SCRIPT_DIR/output.golden
