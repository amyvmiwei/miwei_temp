#!/usr/bin/env bash
#
# Copyright (C) 2007-2014 Hypertable, Inc.
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

FORCE="false"
only="false"

STOP_FSBROKER="false"
STOP_MASTER="false"
STOP_RANGESERVER="false"
STOP_THRIFTBROKER="false"
STOP_HYPERSPACE="false"
STOP_TESTCLIENT="false"
STOP_TESTDISPATCHER="false"

for arg in "$@"
do
  case $arg in
    thriftbroker)
      only="true"
      STOP_THRIFTBROKER="true"
      ;;
    master)
      only="true"
      STOP_MASTER="true"
      ;;
    hyperspace)
      only="true"
      STOP_HYPERSPACE="true"
      ;;
    rangeserver)
      only="true"
      STOP_RANGESERVER="true"
      ;;
    dfsbroker)
      only="true"
      STOP_FSBROKER="true"
      ;;
    fsbroker)
      only="true"
      STOP_FSBROKER="true"
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
  echo ""
  echo "usage: stop-servers.sh [OPTIONS]"
  echo ""
  echo "OPTIONS:"
  echo "  --force                 kill processes to ensure they're stopped"
  echo "  --no-fsbroker           do not stop the FS broker"
  echo "  --no-master             do not stop the Hypertable master"
  echo "  --no-rangeserver        do not stop the RangeServer"
  echo "  --no-hyperspace         do not stop the Hyperspace master"
  echo "  --no-thriftbroker       do not stop the ThriftBroker"
  echo "  fsbroker                stops the FS broker"
  echo "  master                  stops the Hypertable master"
  echo "  rangeserver             stops the RangeServer"
  echo "  hyperspace              stops the Hyperspace master"
  echo "  thriftbroker            stops the ThriftBroker"
  echo ""
}

while [ "$1" != "${1##[-+]}" ]; do
  case $1 in
    '')
      usage
      exit 1;;
    --force)
      FORCE="true"
      shift
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
    --only-dfsbroker)
      STOP_FSBROKER="true"
      shift
      ;;
    --only-fsbroker)
      STOP_FSBROKER="true"
      shift
      ;;
    --only-master)
      STOP_MASTER="true"
      shift
      ;;
    --only-rangeserver)
      STOP_RANGESERVER="true"
      shift
      ;;
    --only-hyperspace)
      STOP_HYPERSPACE="true"
      shift
      ;;
    --only-thriftbroker)
      STOP_THRIFTBROKER="true"
      shift
      ;;
    *)
      usage
      exit 1;;
  esac
done

if [ ! -e $HYPERTABLE_HOME/bin/ht_master_client ] ; then
  STOP_MASTER="false"
fi

if [ ! -e $HYPERTABLE_HOME/bin/ht_rsclient ] ; then
  STOP_RANGESERVER="false"
fi

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
  stop_server thriftbroker
fi

#
# Stop Master
#
if [ $STOP_MASTER == "true" ] ; then
  echo 'shutdown;quit;' | $HYPERTABLE_HOME/bin/ht master_client --batch
  # wait for master shutdown
  wait_for_server_shutdown master "master" "$@"
  if [ $? != 0 ] ; then
      stop_server master
  fi
fi

#
# Stop RangeServer
#
if [ $STOP_RANGESERVER == "true" ] ; then
  echo "Sending shutdown command"
  echo 'shutdown;quit' | $HYPERTABLE_HOME/bin/ht rsclient --batch --no-hyperspace
  # wait for rangeserver shutdown
  wait_for_server_shutdown rangeserver "range server" "$@"
  if [ $? != 0 ] ; then
      stop_server master
  fi
fi

#
# Stop FS Broker
#
if [ $STOP_FSBROKER == "true" ] ; then
  echo "Sending shutdown command to FS broker"
  echo 'shutdown' | $HYPERTABLE_HOME/bin/ht fsclient --nowait --batch
  stop_server fsbroker
fi

#
# Stop Hyperspace
#
if [ $STOP_HYPERSPACE == "true" ] ; then
  stop_server hyperspace 
fi

sleep 1

#
# wait for thriftbroker shutdown
#
if [ $STOP_THRIFTBROKER == "true" ] ; then
  wait_for_server_shutdown thriftbroker "thrift broker" "$@" &
fi

#
# wait for FS Broker shutdown
#
if [ $STOP_FSBROKER == "true" ] ; then
  wait_for_server_shutdown fsbroker "FS broker" "$@" &
fi

#
# wait for master shutdown
#
if [ $STOP_MASTER == "true" ] ; then
  wait_for_server_shutdown master "hypertable master" "$@" &
fi

#
# wait for hyperspace shutdown
#
if [ $STOP_HYPERSPACE == "true" ] ; then
    wait_for_server_shutdown hyperspace "hyperspace" "$@" &
fi

wait
