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
  echo "usage: ht-stop-fsbroker.sh [OPTIONS] [<global-options>]"
  echo
  echo "OPTIONS:"
  echo "  -h,--help  Display usage information"
  echo
  echo "Stops the filesystem broker.  This script attempts to stop the FsBroker by"
  echo "issuing the \"shutdown\" command to the FsBroker CLI (ht fsbroker).  It then"
  echo "repeatedly runs ht-check-fsbroker.sh until it reports status CRITICAL, after"
  echo "which it displays the following message to the console:"
  echo
  echo "  Shutdown FsBroker complete"
  echo
  echo "If after 40 attempts (with a one second wait in between each), the status check"
  echo "does not return CRITICAL, the FsBroker process, whose process ID is found in the "
  echo "\$HT_HOME/run/FsBroker.<fs>.pid file, will be killed by sending it the KILL"
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

echo 'shutdown' | $HYPERTABLE_HOME/bin/ht fsbroker --nowait --batch --silent $@
wait_for_critical fsbroker "FsBroker" "$@"
if [ $? -eq 0 ]; then
  pidfile=`server_pidfile fsbroker`
  \rm -f $pidfile
else
  stop_server fsbroker
fi
