#!/bin/bash
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
  local REAL_HOME=$HYPERTABLE_HOME
  readlink $HYPERTABLE_HOME > /dev/null
  if [ $? -eq 0 ]; then
    REAL_HOME="`dirname $HYPERTABLE_HOME`/`readlink $HYPERTABLE_HOME`"
  fi
  echo
  echo "usage: ht-destroy-hyperspace.sh [OPTIONS] [<server-options>]"
  echo
  echo "OPTIONS:"
  echo "  -h,--help             Display usage information"
  echo "  -i,--interactive      Displays a warning and waits for 10 seconds before"
  echo "                        proceeding to give the operator a chance to hit"
  echo "                        ctrl-c to exit"
  echo
  echo "This script shuts down the Hyperspace process running on localhost"
  echo "and then recursively removes the contents of the following directory:"
  echo
  echo "  $REAL_HOME/hyperspace"
  echo
  echo "The <server-options> arguments are passed into the invocation of the"
  echo "ht-stop-hyperspace.sh"
  echo
}

if [ $# == 1 ]; then
  case $1 in
    -h|--help)
      usage
      exit 0
      ;;
  esac
fi

confirm=y

if tty > /dev/null && [ $# == 1 ]; then
  case $1 in
    -i|--interactive)
      echo "Are you sure you want to destroy Hyperspace (on default ports)?"
      echo "Will proceed in 10 seconds, (Ctrl-C to quit)"
      shift
      sleep 10
      ;;
  esac
fi

# Stop hyperspace
$HYPERTABLE_HOME/bin/ht-stop-hyperspace.sh "$@"
if [ $? -ne 0 ]; then
  exit 2
fi

case $confirm in
  y|Y)
    #
    # Clear state
    #
    /bin/rm -rf $HYPERTABLE_HOME/hyperspace/*
    echo "Cleared hyperspace"
    ;;
  *) echo "Hyperspace not cleared";;
esac
