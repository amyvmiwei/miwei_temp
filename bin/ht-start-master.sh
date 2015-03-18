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
  echo "usage: ht-start-master.sh [OPTIONS] [<master-options>]"
  echo
  echo "OPTIONS:"
  echo "  -h,--help    Display usage information"
  echo "  --heapcheck  Run master with heap checking enabled"
  echo "  --valgrind   Run master with valgrind"
  echo
  echo "Starts the Master on localhost.  It first checks to see if the Master process is"
  echo "already running by comparing the process ID stored in \$HT_HOME/run/Master.pid"
  echo "with the process IDs of the currently running processes.  If there's no match,"
  echo "it then performs a second check with ht-check-master.sh.  If the Master appears"
  echo "to be running, the script exits with status 0.  Otherwise, the Master process"
  echo "(htMaster) is launched with the options --verbose and <master-options>."
  echo
  echo "If the --valgrind option is supplied, then the Master process is run with"
  echo "valgrind as follows:"
  echo
  echo "  valgrind -v --log-file=vg.master.%p --leak-check=full \\"
  echo "           --num-callers=20 htMaster ..."
  echo
  echo "If the --heapcheck option is supplied, then the Master process is run with"
  echo "heap checking enabled as follows:"
  echo
  echo "  env HEAPCHECK=normal htMaster ..."
  echo
  echo "The output of Master is piped into cronolog, a log rotation program, which"
  echo "directs its output to the log subdirectory within the Hypertable installation"
  echo "directory (HT_HOME).  The main log file is named Master.log and archives are"
  echo "stored in the \$HT_HOME/log/archive directory.  The following illustrates what"
  echo "the contents of the \$HT_HOME/log directory might look like for the Master logs:"
  echo
  echo "  archive/2015-02/28/Master.log"
  echo "  archive/2015-03/01/Master.log"
  echo "  archive/2015-03/02/Master.log"
  echo "  Master.log -> archive/2015-03/02/Master.log"
  echo
  echo "After launching the Master process, the ht-check-master.sh script is run"
  echo "repeatedly until the status is OK at which point it displays a message such as"
  echo "the followng to the console and the script exits with status code 0."
  echo
  echo "  Master Started"
  echo
  echo "If after 40 attempts (with a one second wait in between each), the status check"
  echo "does not return OK, the status text will be written to the terminal and the"
  echo "status code is returned as the exit status of the script."
  echo
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
  exec_server htMaster --verbose "$@"
  wait_for_ready master "Master" "$@"
else
  echo "WARNING: Master already running."
fi
