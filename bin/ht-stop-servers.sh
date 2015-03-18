#!/usr/bin/env bash
#
# Copyright (C) 2007-2015 Hypertable, Inc.
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 3 of the
# License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh

only="false"

STOP_FSBROKER="false"
STOP_MASTER="false"
STOP_RANGESERVER="false"
STOP_THRIFTBROKER="false"
STOP_HYPERSPACE="false"
STOP_TESTCLIENT="false"
STOP_TESTDISPATCHER="false"

while [ $# -gt 0 ]; do
  case $1 in
    thriftbroker)
      only="true"
      STOP_THRIFTBROKER="true"
      shift
      ;;
    master)
      only="true"
      STOP_MASTER="true"
      shift
      ;;
    hyperspace)
      only="true"
      STOP_HYPERSPACE="true"
      shift
      ;;
    rangeserver)
      only="true"
      STOP_RANGESERVER="true"
      shift
      ;;
    dfsbroker)
      only="true"
      STOP_FSBROKER="true"
      shift
      ;;
    fsbroker)
      only="true"
      STOP_FSBROKER="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

if [ $only == "false" ]; then
  STOP_FSBROKER="true"
  STOP_MASTER="true"
  STOP_RANGESERVER="true"
  STOP_THRIFTBROKER="true"
  STOP_HYPERSPACE="true"
  STOP_TESTCLIENT="true"
  STOP_TESTDISPATCHER="true"
fi

usage() {
  echo
  echo "usage: ht-stop-servers.sh [OPTIONS] [<global-options>]"
  echo
  echo "OPTIONS:"
  echo "  -h,--help          Display usage information"
  echo "  --no-fsbroker      Do not stop the FsBroker"
  echo "  --no-master        Do not stop the Master"
  echo "  --no-rangeserver   Do not stop the RangeServer"
  echo "  --no-hyperspace    Do not stop the Hyperspace replica"
  echo "  --no-thriftbroker  Do not stop the ThriftBroker"
  echo "  fsbroker           Stop the FsBroker"
  echo "  master             Stop the Master"
  echo "  rangeserver        Stop the RangeServer"
  echo "  hyperspace         Stop the Hyperspace replica"
  echo "  thriftbroker       Stop the ThriftBroker"
  echo
  echo "Stops all Hypertable processes running on localhost.  The processes are stopped"
  echo "in the following order:"
  echo
  echo "  ThriftBroker"
  echo "  Master"
  echo "  RangeServer"
  echo "  FsBroker"
  echo "  Hyperspace"
  echo
  echo "If any process specifier options are supplied (i.e. fsbroker, master,"
  echo "rangeserver, hyperspace, thriftbroker), then the only processes stopped are the"
  echo "ones that match the specifiers.  Otherwise, all processes are stopped except for"
  echo "the ones specified with --no options."
  echo
  echo "The <global-options> are passed to the ht-stop-* scripts."
  echo
  echo "The exit status of the script is 0 if all specified processes were successfully"
  echo "stopped, otherwise the script exits with status 1."
  echo
}

while [ $# -gt 0 ]; do
  case $1 in
    -h|--help)
      usage
      exit 0
      ;;
    --no-fsbroker)
      STOP_FSBROKER="false"
      shift
      ;;
    --no-dfsbroker)
      STOP_FSBROKER="false"
      shift
      ;;
    --no-master)
      STOP_MASTER="false"
      shift
      ;;
    --no-rangeserver)
      STOP_RANGESERVER="false"
      shift
      ;;
    --no-hyperspace)
      STOP_HYPERSPACE="false"
      shift
      ;;
    --no-thriftbroker)
      STOP_THRIFTBROKER="false"
      shift
      ;;
    *)
      break
      ;;
  esac
done

RET=0

#
# Stop TestClient
#
if [ $STOP_TESTCLIENT == "true" ] ; then
  stop_server testclient
fi

#
# Stop TestDispatcher
#
if [ $STOP_TESTDISPATCHER == "true" ] ; then
  stop_server testdispatcher
fi

#
# Stop Thriftbroker 
#
if [ $STOP_THRIFTBROKER == "true" ] ; then
  $HYPERTABLE_HOME/bin/ht-stop-thriftbroker.sh "$@"
  if [ $? -ne 0 ]; then
    RET=1
  fi
fi

#
# Stop Master
#
if [ $STOP_MASTER == "true" ] ; then
  $HYPERTABLE_HOME/bin/ht-stop-master.sh "$@"
  if [ $? -ne 0 ]; then
    RET=1
  fi
fi

#
# Stop RangeServer
#
if [ $STOP_RANGESERVER == "true" ] ; then
  $HYPERTABLE_HOME/bin/ht-stop-rangeserver.sh "$@"
  if [ $? -ne 0 ]; then
    RET=1
  fi
fi

#
# Stop FsBroker
#
if [ $STOP_FSBROKER == "true" ] ; then
  $HYPERTABLE_HOME/bin/ht-stop-fsbroker.sh "$@"
  if [ $? -ne 0 ]; then
    RET=1
  fi
fi

#
# Stop Hyperspace
#
if [ $STOP_HYPERSPACE == "true" ] ; then
  $HYPERTABLE_HOME/bin/ht-stop-hyperspace.sh "$@"
  if [ $? -ne 0 ]; then
    RET=1
  fi
fi

exit $RET
