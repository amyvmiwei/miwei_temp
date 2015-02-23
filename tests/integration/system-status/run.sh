#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

function on_exit() {
    $HT_HOME/bin/ht-destroy-database.sh
}

trap on_exit EXIT

status_string_to_code () {
  if [ $1 == "OK" ]; then
    return 0
  elif [ $1 == "WARNING" ]; then
    return 1
  elif [ $1 == "CRITICAL" ]; then
    return 2
  elif [ $1 == "UNKNOWN" ]; then
    return 3
  fi
  echo "Error - unknown status string: $1"
  exit 1
}

\rm -f output
$HT_HOME/bin/ht-destroy-database.sh

$HT_HOME/bin/ht-check.sh >> output
ret=$?
if [ $ret -ne 2 ]; then
  echo "(1) Status should be CRITICAL"
  exit 1
fi

$HT_HOME/bin/ht-start-hyperspace.sh
$HT_HOME/bin/ht-check.sh >> output
ret=$?
if [ $ret -ne 2 ]; then
  echo "(2) Status is $ret when it should be CRITICAL"
  exit 1
fi

$HT_HOME/bin/ht-start-fsbroker.sh local
$HT_HOME/bin/ht-start-master.sh
$HT_HOME/bin/ht-check.sh >> output
ret=$?
if [ $ret -ne 2 ]; then
  echo "(3) Status is $ret when it should be CRITICAL"
  exit 1
fi

RS_READY_STATUS=`$HT_HOME/bin/ht get_property Hypertable.RangeServer.ReadyStatus | sed 's/"//g'`
status_string_to_code $RS_READY_STATUS
RS_READY_CODE=$?

$HT_HOME/bin/ht-start-rangeserver.sh
$HT_HOME/bin/ht-check.sh >> output
ret=$?
if [ $ret -gt $RS_READY_CODE ]; then
  echo "(4) Status is $ret when it should be $RS_READY_STATUS"
  exit 1
fi


kill -STOP `cat $HT_HOME/run/RangeServer.pid`
$HT_HOME/bin/ht-check.sh -t 7 >> output
ret=$?
if [ $ret -ne 1 ]; then
  echo "(5) Status is $ret when it should be WARNING"
  exit 1
fi
kill -CONT `cat $HT_HOME/run/RangeServer.pid`

# NOTE: The output line of the next two tests are unstable which
#       is why we're not capturing them in the output file

$HT_HOME/bin/ht-stop-rangeserver.sh
$HT_HOME/bin/ht-check.sh
ret=$?
if [ $ret -ne 2 ]; then
  echo "(6) Status is $ret when it should be CRITICAL"
  exit 1
fi

$HT_HOME/bin/ht-start-rangeserver.sh
$HT_HOME/bin/ht-stop-fsbroker.sh
$HT_HOME/bin/ht-check.sh
ret=$?
if [ $ret -ne 2 ]; then
  echo "(7) Status is $ret when it should be CRITICAL"
  exit 1
fi

cat output | sed 's/\([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\)//g' > output.stripped

diff output.stripped $SCRIPT_DIR/output.golden
