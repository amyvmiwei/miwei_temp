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

SCRIPT_DIR=`dirname $0`

usage() {
  echo ""
  echo "usage: xdeps.sh [OPTIONS] <exename> [ <exename> ... ]"
  echo ""
  echo "OPTIONS:"
  echo "  -h,--help             Display usage information"
  echo ""
  echo "This script generates of a list of the unique shared library"
  echo "dependencies across all of the executables listed on the"
  echo "command line."
  echo ""
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

if [ $# -eq 0 ]; then
  usage;
  exit 0
fi

(
  for f in $@; do
    $SCRIPT_DIR/ldd.sh $f
  done
) | fgrep -v hypertable | cut -f2 | cut -f1 -d'(' | sort | uniq

exit 0
