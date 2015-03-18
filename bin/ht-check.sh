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
  echo
  echo "usage: ht-check.sh [OPTIONS] [<server-options>]"
  echo
  echo "OPTIONS:"
  echo "  -h,--help             Display usage information"
  echo "  -t,--timeout <sec>    Timeout after <sec> seconds (default = 20)"
  echo
  echo "Checks the overall status Hypertable.  This script is Nagios plugin"
  echo "compliant and communicates the status of the system by returning one of"
  echo "the codes in the following table as its exit status."
  echo
  echo "  Code  Status    Description"
  echo "  ----  ------    -----------"
  echo "  0     OK        Up and operating properly"
  echo "  1     WARNING   Up and operating, but needs attention"
  echo "  2     CRITICAL  Down or not working properly"
  echo "  3     UNKNOWN   Unable to determine status"
  echo
  echo "In addition to the exit status, this script will also write a human-readable"
  echo "description of the status to the terminal in the following format:"
  echo
  echo "  Hypertable <status> - <description>"
  echo
  echo "This script performs an overall Hypertable status check, which includes"
  echo "checks of the following processes:"
  echo
  echo "  Hyperspace"
  echo "  Master (active)"
  echo "  RangeServers"
  echo "  FsBrokers (on active Master and all RangeServers)"
  echo
  echo "The status check will stop on the first non-OK status it encounters and will"
  echo "report that status, along with its description, as the Hypertable status."
  echo
  echo "NOTE: This script does not perform checks on the ThriftBrokers."
  echo "To perform checks on the ThriftBroker processes, the ht-check-thrifbroker.sh"
  echo "script should be run on each ThriftBroker machine independently."
  echo
}

TIMEOUT_OPTION="--timeout 20000"
SERVICE_HOSTNAME=localhost

while [ $# -gt 0 ]; do
  case $1 in
    -h|--help)
      usage;
      exit 0
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

$RUNTIME_ROOT/bin/ht shell ${TIMEOUT_OPTION} --batch --silent-startup -e "status"
