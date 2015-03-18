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

usage() {
  echo
  echo "usage: ht-stop-rangeserver.sh [OPTIONS] [<global-options>]"
  echo
  echo "OPTIONS:"
  echo "  -h,--help  Display usage information"
  echo
  echo "Stops the RangeServer process.  This script attempts to stop the RangeServer by"
  echo "issuing the \"shutdown\" command to the RangeServer CLI (ht rangeserver).  It"
  echo "then repeatedly runs ht-check-rangeserver.sh until it reports status CRITICAL,"
  echo "after which it displays the following message to the console:"
  echo
  echo "  Shutdown RangeServer complete"
  echo
  echo "If after 40 attempts (with a one second wait in between each), the status check"
  echo "does not return CRITICAL, the RangeServer process, whose process ID is found in"
  echo "the \$HT_HOME/run/RangeServer.pid file, will be killed by sending it the KILL"
  echo "signal."
  echo
  echo "The <global-options> are passed to all programs run by this script and the exit"
  echo "status is 0 under all circumstances."
  echo
}

while [ $# -gt 0 ]; do
  case $1 in
    -h|--help)
      usage;
      exit 0
      ;;
    *)
      break
      ;;
  esac
done

echo 'shutdown' | $HYPERTABLE_HOME/bin/ht rangeserver --batch --silent --no-hyperspace $@
wait_for_critical rangeserver "RangeServer" "$@"
if [ $? -ne 0 ]; then
  stop_server rangeserver
fi
