#!/usr/bin/env bash
#
# Copyright (C) 2007-2012 Hypertable, Inc.
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

ldd_if_exists() {
  if [ -e $1 ] ; then
    $HYPERTABLE_HOME/bin/ldd.sh $1
  fi
}


{
ldd_if_exists $HYPERTABLE_HOME/bin/Hyperspace.Master
ldd_if_exists $HYPERTABLE_HOME/bin/Hypertable.Master
ldd_if_exists $HYPERTABLE_HOME/bin/Hypertable.RangeServer
ldd_if_exists $HYPERTABLE_HOME/bin/ThriftBroker
ldd_if_exists $HYPERTABLE_HOME/bin/csdump
ldd_if_exists $HYPERTABLE_HOME/bin/fsclient
ldd_if_exists $HYPERTABLE_HOME/bin/dumplog
ldd_if_exists $HYPERTABLE_HOME/bin/hyperspace
ldd_if_exists $HYPERTABLE_HOME/bin/hypertable
ldd_if_exists $HYPERTABLE_HOME/bin/localBroker
ldd_if_exists $HYPERTABLE_HOME/bin/metalog_dump
ldd_if_exists $HYPERTABLE_HOME/bin/serverup
ldd_if_exists $HYPERTABLE_HOME/bin/system_info
} | fgrep -v hypertable | cut -f1 -d'(' | sort | uniq
