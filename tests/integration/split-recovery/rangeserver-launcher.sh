#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME
PIDFILE=$HT_HOME/run/RangeServer.pid
LAUNCHER_PIDFILE=$HT_HOME/run/RangeServerLauncher.pid
DUMP_METALOG="$HT_HOME/bin/ht metalog_dump"
METALOG="/hypertable/servers/rs1/log/rsml/"
RANGE_SIZE=${RANGE_SIZE:-"7M"}

. $HT_HOME/bin/ht-env.sh

# Kill launcher if running & store pid of this launcher
if [ -f $LAUNCHER_PIDFILE ]; then
  kill -9 `cat $LAUNCHER_PIDFILE`
  \rm -f $LAUNCHER_PIDFILE
fi
echo "$$" > $LAUNCHER_PIDFILE

# Kill RangeServer if running
if [ -f $PIDFILE ]; then
  kill -9 `cat $PIDFILE`
  \rm -f $PIDFILE
fi

$HT_HOME/bin/htRangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.RangeServer.UpdateDelay=100 \
    --Hypertable.RangeServer.Range.SplitSize=$RANGE_SIZE $@

# Exit if base run
if [ -z $1 ]; then
    \rm -f $LAUNCHER_PIDFILE
    exit
fi

echo ""
echo "!!!! CRASH ($@) !!!!"
echo ""

echo "RSML entries:"
$DUMP_METALOG $METALOG
echo "Range states:"
$DUMP_METALOG --all $METALOG

#$HT_HOME/bin/ht valgrind -v --log-file=vg_rs1.log --track-origins=yes \
$HT_HOME/bin/htRangeServer --pidfile=$PIDFILE --verbose

\rm -f $LAUNCHER_PIDFILE
