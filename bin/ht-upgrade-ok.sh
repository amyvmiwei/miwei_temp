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
  echo "usage: ht-upgrade-ok.sh [OPTIONS] <from> <to>"
  echo
  echo "OPTIONS:"
  echo "  -h,--help     Display usage information"
  echo "  -v,--verbose  Display verbose output"
  echo
  echo "Determines whether or not the upgrade from Hypertable version <from> to version"
  echo "<to> is valid.  <from> and <to> are assumed to be Hypertable installation"
  echo "directories whose last path component is either a version number or the symbolic"
  echo "link \"current\" which points to a Hypertable installation directory whose last"
  echo "path component is a version number."
  echo
  echo "The script returns with exit status 0 if the upgrade is compatible, otherwise it"
  echo "returns with exit status 1."
  echo
}

VERBOSE=

while [ $# -gt 0 ]; do
  case $1 in
    -h|--help)
      usage
      exit 0
      ;;
    -v|--verbose)
      VERBOSE=true
      shift
      ;;
    *)
      break
      ;;
  esac
done

if [ $# != 2 ] ; then
  usage
  exit 0
fi

FROM=`readlink $1`
if [ $? -ne 0 ]; then
  FROM=$1
fi
FROM=`basename $FROM`

TO=`readlink $2`
if [ $? -ne 0 ]; then
  TO=$2
fi
TO=`basename $TO`

if [ "$FROM" == "$TO" ] ; then
  exit 0
fi

MAJOR=`echo $FROM | cut -d'.' -f1`
MINOR=`echo $FROM | cut -d'.' -f2`
MICRO=`echo $FROM | cut -d'.' -f3`
PATCH=`echo $FROM | cut -d'.' -f4`

if [ -z "$MAJOR" ] || [ -z "$MINOR" ] || [ -z "$MICRO" ] || [ -z "$PATCH" ] ; then
  echo "Unable to extract version number from <from> argument: $1"
  exit 1
fi

let "FROM_NUMBER=(MAJOR*10000000)+(MINOR*100000)+(MICRO*1000)+PATCH"

MAJOR=`echo $TO | cut -d'.' -f1`
MINOR=`echo $TO | cut -d'.' -f2`
MICRO=`echo $TO | cut -d'.' -f3`
PATCH=`echo $TO | cut -d'.' -f4`

if [ -z "$MAJOR" ] || [ -z "$MINOR" ] || [ -z "$MICRO" ] || [ -z "$PATCH" ] ; then
  echo "Unable to extract version number from <from> argument: $1"
  exit 1
fi

let "TO_NUMBER=(MAJOR*10000000)+(MINOR*100000)+(MICRO*1000)+PATCH"

if [ -n "$VERBOSE" ]; then
  echo "FROM = $FROM ($FROM_NUMBER)"
  echo "TO   = $TO ($TO_NUMBER)"
fi

RET=0

if [ $FROM_NUMBER -ge 908005 ] && [ $TO_NUMBER -lt 908005 ]; then
  RET=1
fi

if [ $RET -eq 1 ]; then
  echo "Incompatible upgrade: $FROM -> $TO"
fi

exit $RET
