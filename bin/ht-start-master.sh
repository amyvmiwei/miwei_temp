#!/usr/bin/env bash
#
# Copyright (C) 2007-2015 Hypertable, Inc.
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

usage() {
  echo ""
  echo "usage: ht-start-master.sh [OPTIONS] [<server-options>]"
  echo ""
  echo "OPTIONS:"
  echo "  --valgrind   Run master with valgrind"
  echo "  --heapcheck  Run master with google-perf-tools Heapcheck"
  echo ""
}

while [ $# -gt 0 ]; do
  case $1 in
    --valgrind)
      VALGRIND="valgrind -v --log-file=vg.master.%p --leak-check=full --num-callers=20 "
      shift
      ;;
    --heapcheck)
      HEAPCHECK="env HEAPCHECK=normal "
      shift
      ;;
    -h|--help)
      usage;
      exit 0
      ;;
    *)
      break
      ;;
  esac
done

set_start_vars Master
check_pidfile $pidfile && exit 0

$HYPERTABLE_HOME/bin/ht-check-master.sh --silent "$@"
if [ $? != 0 ] ; then
  exec_server Hypertable.Master --verbose "$@"
  wait_for_ok master "Master" "$@"
else
  echo "WARNING: Master already running."
fi
