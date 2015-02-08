#!/usr/bin/env bash
#
# Copyright (C) 2007-2015 Hypertable, Inc.
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Hypertable. If not, see <http://www.gnu.org/licenses/>
#

export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh

if [ "e$RUNTIME_ROOT" == "e" ]; then
  RUNTIME_ROOT=$HYPERTABLE_HOME
fi

usage() {
  echo ""
  echo "usage: ht-check-fsbroker.sh [OPTIONS] [<server-options>]"
  echo ""
  echo "OPTIONS:"
  echo "  -h,--help             Display usage information"
  echo "  -t,--timeout <sec>    Timeout after <sec> seconds (default = 5)"
  echo "  -H,--hostname <addr>  Hostname or IP address of service (default = localhost)"
  echo ""
}

TIMEOUT_OPTION="--timeout 5000"
SERVICE_HOSTNAME=localhost

while [ $# -gt 0 ]; do
  case $1 in
    -h|--help)
      usage;
      exit 0
      ;;
    -H|--hostname)
      shift
      if [ $# -eq 0 ]; then
        usage;
        exit 0
      fi
      SERVICE_HOSTNAME=$1
      shift
      ;;
    -t|--timeout)
      shift
      if [ $# -eq 0 ]; then
        usage;
        exit 0
      fi
      let MILLISECONDS=$1*1000
      TIMEOUT_OPTION="--timeout $MILLISECONDS"
      shift
      ;;
    *)
      break
      ;;
  esac
done

$RUNTIME_ROOT/bin/ht fsbroker --batch ${TIMEOUT_OPTION} -e "status" "$@" ${SERVICE_HOSTNAME}
