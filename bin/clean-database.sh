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

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh

if [ "e$RUNTIME_ROOT" == "e" ]; then
  RUNTIME_ROOT=$HYPERTABLE_HOME
fi

confirm=y

if tty > /dev/null && [ $# == 1 ]; then
  case $1 in
    -i|--interactive)
      echo "Are you sure you want to clear the database (on default ports)?"
      echo "Will proceed in 10 seconds, (Ctrl-C to quit)"
      shift
      sleep 10
      ;;
  esac
fi

# Stop servers other than fsbroker
stop_server thriftbroker rangeserver master hyperspace
sleep 1
wait_for_server_shutdown thriftbroker "thrift broker" "$@" &
wait_for_server_shutdown rangeserver "range server" "$@" &
wait_for_server_shutdown master "hypertable master" "$@" &
wait_for_server_shutdown hyperspace "hyperspace" "$@" &

case $confirm in
  y|Y)
    #
    # Clear state
    #
    check_server "$@" fsbroker 
    if [ $? != 0 ] ; then
      echo "ERROR: FsBroker not running, database not cleaned"
      # remove local stuff anyway.
      rm -rf $RUNTIME_ROOT/hyperspace/* $RUNTIME_ROOT/fs/* $RUNTIME_ROOT/run/log_backup/rsml/* $RUNTIME_ROOT/run/log_backup/mml/*
      exit 1
    fi

    TOPLEVEL="/"`$HYPERTABLE_HOME/bin/get_property $@ Hypertable.Directory`"/"
    TOPLEVEL=`echo $TOPLEVEL | tr -s "/" | sed 's/.$//g'`

    $HYPERTABLE_HOME/bin/fsclient --timeout 60000 --eval "rmdir $TOPLEVEL/servers" "$@"
    $HYPERTABLE_HOME/bin/fsclient --timeout 60000 --eval "rmdir $TOPLEVEL/tables" "$@"
    echo "Removed $TOPLEVEL/servers in FS"
    echo "Removed $TOPLEVEL/tables in FS"
    /bin/rm -rf $RUNTIME_ROOT/hyperspace/*
    /bin/rm -rf $RUNTIME_ROOT/run/log_backup/rsml/*
    /bin/rm -rf $RUNTIME_ROOT/run/log_backup/mml/*
    /bin/rm -rf $RUNTIME_ROOT/run/location
    /bin/rm -rf $RUNTIME_ROOT/run/last-fs
    echo "Cleared hyperspace"
    #/bin/rm -rf $RUNTIME_ROOT/run/monitoring/*
    #echo "Cleared monitoring data"
    ;;
  *) echo "Database not cleared";;
esac

#
# Stop fsbroker
#
stop_server fsbroker
sleep 1
wait_for_server_shutdown fsbroker "FS broker" "$@" &
wait
