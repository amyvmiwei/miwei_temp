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
  echo "usage: ht-start-rangeserver.sh [OPTIONS] [<server-options>]"
  echo ""
  echo "OPTIONS:"
  echo "  --valgrind   Run rangeserver with valgrind"
  echo "  --heapcheck  Run rangeserver with google-perf-tools Heapcheck"
  echo ""
}

while [ $# -gt 0 ]; do
  case $1 in
    --valgrind)
      VALGRIND="valgrind -v --log-file=vg.rangeserver.%p --leak-check=full --num-callers=20 "
      shift
      ;;
    --heapcheck)
      HEAPCHECK="env HEAPCHECK=normal "
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      break
      ;;
  esac
done

set_start_vars RangeServer
check_pidfile $pidfile && exit 0

$HYPERTABLE_HOME/bin/ht-check-rangeserver.sh --silent "$@"
if [ $? != 0 ] ; then
  exec_server Hypertable.RangeServer --verbose "$@"
  max_retries=3600
  report_interval=30
  retries=20
  wait_for_ok rangeserver "RangeServer" "$@"
else
  echo "WARNING: RangeServer already running."
fi
