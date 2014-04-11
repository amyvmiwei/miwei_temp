#!/usr/bin/env bash
#
# Copyright (C) 2007-2012 Hypertable, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

echo "FS broker: available file descriptors: `ulimit -n`"

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh

usage() {
  echo ""
  echo "usage: start-dfsbroker.sh [OPTIONS] (local|hadoop|mapr|ceph|qfs) [<global-args>]"
  echo ""
  echo "OPTIONS:"
  echo "  --valgrind  run broker with valgrind"
  echo ""
}

fs_conflict_error() {
    OLD_FS=$1
    shift
    NEW_FS=$1
    echo ""
    echo "ERROR: FS conflict"
    echo ""
    echo "You are trying to run Hypertable with the '$NEW_FS' broker"
    echo "on a system that was previously run with the '$OLD_FS' broker."
    echo ""
    if [ "$OLD_FS" == "local" ] ; then
        echo "Run the following command to remove the previous database,"
        echo "and all of its associated state, before launching with the"
        echo "'$NEW_FS' broker:"
        echo ""
        echo "$HYPERTABLE_HOME/bin/stop-servers.sh ; $HYPERTABLE_HOME/bin/start-fsbroker.sh $OLD_FS ; $HYPERTABLE_HOME/bin/clean-database.sh"
        echo ""
    else
        echo "To remove the previous database, and all it's associated state,"
        echo "in order to launch with the '$NEW_FS' broker, start the system"
        echo "on the old FS and then clean the database.  For example, with"
        echo "Capistrano:"
        echo ""
        echo "cap stop ; cap -S fs=$OLD_FS cleandb"
        echo ""
    fi
    echo "Alternatively, you can manually purge the database state by issuing"
    echo "the following command on each Master and Hyperspace replica machine:"
    echo ""
    echo "rm -rf $HYPERTABLE_HOME/hyperspace/* $HYPERTABLE_HOME/fs/* $HYPERTABLE_HOME/run/rsml_backup/* $HYPERTABLE_HOME/run/last-fs"
    echo ""
}

while [ "$1" != "${1##[-+]}" ]; do
  case $1 in
    --valgrind)
      VALGRIND="valgrind -v --log-file=vg.fsbroker.%p --leak-check=full --num-callers=20 "
      shift
      ;;
    *)
      usage
      exit 1;;
  esac
done

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

FS=$1
shift

if [ -e $HYPERTABLE_HOME/run/last-fs ] ; then
    LAST_FS=`cat $HYPERTABLE_HOME/run/last-fs`
    if [ "$FS" != "$LAST_FS" ] ; then
        fs_conflict_error $LAST_FS $FS
        exit 1
    fi
else
    # record last FS
    echo $FS > $HYPERTABLE_HOME/run/last-fs
fi

set_start_vars FsBroker.$FS
check_pidfile $pidfile && exit 0

check_server "$@" fsbroker 
if [ $? != 0 ] ; then
  if [ "$FS" == "hadoop" ] ; then
    if [ "n$VALGRIND" != "n" ] ; then
      echo "ERROR: hadoop broker cannot be run with valgrind"
      exit 1
    fi
    exec_server jrun org.hypertable.FsBroker.hadoop.main --verbose "$@"
  elif [ "$FS" == "mapr" ] ; then
    exec_server maprBroker --verbose "$@"
  elif [ "$FS" == "ceph" ] ; then
    exec_server cephBroker --verbose "$@"
  elif [ "$FS" == "local" ] ; then
    exec_server localBroker --verbose "$@"
  elif [ "$FS" == "qfs" ] ; then
    exec_server qfsBroker --verbose "$@"
  else
    usage
    exit 1
  fi

  wait_for_server_up fsbroker "FS Broker ($FS)" "$@"
else
  echo "WARNING: FSBroker already running."
fi
