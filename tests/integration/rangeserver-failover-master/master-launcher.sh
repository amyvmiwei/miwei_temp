#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME
PIDFILE=$HT_HOME/run/Master.pid
LAUNCHER_PIDFILE=$HT_HOME/run/MasterLauncher.pid
DUMP_METALOG="$HT_HOME/bin/ht metalog_dump"
METALOG="/hypertable/servers/master/log/mml"
SCRIPT_DIR=`dirname $0`
RESTART_RANGESERVERS=0

. $HT_HOME/bin/ht-env.sh

stop_range_servers() {
    local port
    let port=38059+$1
    while [ $port -ge 38060 ] ; do
        echo "shutdown; quit;" | $HT_HOME/bin/ht rangeserver localhost:$port
        let port-=1
    done
    sleep 1
    kill -9 `cat $HT_HOME/run/RangeServer.rs?.pid`
    \rm -f $HT_HOME/run/RangeServer.rs?.pid
}

# Kill launcher if running & store pid of this launcher
if [ -f $LAUNCHER_PIDFILE ]; then
  kill -9 `cat $LAUNCHER_PIDFILE`
  rm -f $LAUNCHER_PIDFILE
fi
echo "$$" > $LAUNCHER_PIDFILE

# Kill Master if running
if [ -f $PIDFILE ]; then
  kill -9 `cat $PIDFILE`
  rm -f $PIDFILE
fi

# Dumping cores slows things down unnecessarily for normal test runs
#ulimit -c 0

# Check for --restart-rangeserver flag and set restart rangeserver count
if [ $1 == "--restart-rangeservers" ]; then
    shift
    RESTART_RANGESERVERS=$1
    shift
fi

$HT_HOME/bin/htMaster --verbose --pidfile=$PIDFILE \
    --config=${SCRIPT_DIR}/test.cfg $1

# Exit if base run
if [ -z $1 ]; then
    \rm -f $LAUNCHER_PIDFILE
    exit
fi

echo ""
echo "!!!! CRASH ($@) !!!!"
echo ""

echo "MML entries:"
$DUMP_METALOG $METALOG

# Restart RangeServers if requested
if [ $RESTART_RANGESERVERS -ne 0 ]; then
  stop_range_servers $RESTART_RANGESERVERS
  let j=1
  while [ $j -le $RESTART_RANGESERVERS ] ; do
      let port=38059+$j
      $HT_HOME/bin/ht RangeServer --verbose \
          --pidfile=$HT_HOME/run/RangeServer.rs$j.pid \
          --Hypertable.RangeServer.ProxyName=rs$j \
          --Hypertable.RangeServer.Port=$port \
          --config=${SCRIPT_DIR}/test.cfg 2>&1 >> rangeserver.rs$j.output.$TEST &
    let j++
  done
fi


#$HT_HOME/bin/ht valgrind -v --log-file=vg_rs1.log --track-origins=yes \
$HT_HOME/bin/htMaster --pidfile=$PIDFILE --verbose \
    --config=${SCRIPT_DIR}/test.cfg

\rm -f $LAUNCHER_PIDFILE