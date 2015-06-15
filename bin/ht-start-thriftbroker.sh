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

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh

usage() {
  echo
  echo "usage: ht-start-thriftbroker.sh [OPTIONS] [<thriftbroker-options>]"
  echo
  echo "OPTIONS:"
  echo "  -h,--help    Display usage information"
  echo "  --heapcheck  Run thriftbroker with heap checking enabled"
  echo "  --valgrind   Run thriftbroker with valgrind"
  echo
  echo "Starts the ThriftBroker on localhost.  It first checks to see if the"
  echo "ThriftBroker process is already running by comparing the process ID stored in"
  echo "\$HT_HOME/run/ThriftBroker.pid with the process IDs of the currently running"
  echo "processes.  If there's no match, it then performs a second check with"
  echo "ht-check-thriftbroker.sh.  If the ThriftBroker appears to be running, the script"
  echo "exits with status 0.  Otherwise, the ThriftBroker process (htThriftBroker) is"
  echo "launched with the options --verbose and <thriftbroker-options>."
  echo
  echo "If the --valgrind option is supplied, then the ThriftBroker process is run with"
  echo "valgrind as follows:"
  echo
  echo "  valgrind -v --log-file=vg.thriftbroker.%p --leak-check=full \\"
  echo "           --num-callers=50 htThriftBroker ..."
  echo
  echo "If the --heapcheck option is supplied, then the ThriftBroker process is run with"
  echo "heap checking enabled as follows:"
  echo
  echo "  env HEAPCHECK=normal htThriftBroker ..."
  echo
  echo "The output of ThriftBroker is piped into cronolog, a log rotation program, which"
  echo "directs its output to the log subdirectory within the Hypertable installation"
  echo "directory (HT_HOME).  The main log file is named ThriftBroker.log and archives"
  echo "are stored in the \$HT_HOME/log/archive directory.  The following illustrates"
  echo "what the contents of the \$HT_HOME/log directory might look like for the"
  echo "ThriftBroker logs:"
  echo
  echo "  archive/2015-02/28/ThriftBroker.log"
  echo "  archive/2015-03/01/ThriftBroker.log"
  echo "  archive/2015-03/02/ThriftBroker.log"
  echo "  ThriftBroker.log -> archive/2015-03/02/ThriftBroker.log"
  echo
  echo "After launching the ThriftBroker process, the ht-check-thriftbroker.sh script is"
  echo "run repeatedly until the status is OK at which point it displays a message such"
  echo "as the followng to the console and the script exits with status code 0."
  echo
  echo "  ThriftBroker Started"
  echo
  echo "If after 40 attempts (with a one second wait in between each), the status check"
  echo "does not return OK, the status text will be written to the terminal and the"
  echo "status code is returned as the exit status of the script."
  echo
}

while [ $# -gt 0 ]; do
  case $1 in
    --valgrind)
      VALGRIND="valgrind -v --log-file=vg.thriftbroker.%p --leak-check=full --num-callers=50 "
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

set_start_vars ThriftBroker
check_pidfile $pidfile && exit 0

$HYPERTABLE_HOME/bin/ht-check-thriftbroker.sh --silent "$@"
if [ $? != 0 ] ; then
  exec_server htThriftBroker --verbose "$@"
  report_interval=8
  wait_for_ready thriftbroker "ThriftBroker" "$@"
else
  echo "WARNING: ThriftBroker already running."
fi
