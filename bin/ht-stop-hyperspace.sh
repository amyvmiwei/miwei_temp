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
  echo "usage: ht-stop-hyperspace.sh [OPTIONS] [<global-options>]"
  echo
  echo "OPTIONS:"
  echo "  -h,--help  Display usage information"
  echo
  echo "Stops the Hyperspace process running on localhost.  The Hyperspace process,"
  echo "whose process ID is found in the $HT_HOME/run/Hyperspace.pid file, is killed by"
  echo "sending it the KILL signal.  It then runs ht-check-hyperspace.sh to verify"
  echo "that the status is CRITICAL, after which it displays the following message to"
  echo "the console:"
  echo
  echo "  Shutdown Hyperspace complete"
  echo
  echo "If ht-check-hyperspace.sh reports a status other than CRITICAL, the status text"
  echo "is displayed to the terminal and the status code is returned as the exit status."
  echo "The final status check is for cases where the .pid file was inadvertently"
  echo "deleted so that the script will report that the process was not stopped."
  echo
  echo "The <global-options> are passed to ht-check-hyperspace.sh."
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

stop_server hyperspace
max_retries=1
wait_for_critical hyperspace "Hyperspace" "$@"

