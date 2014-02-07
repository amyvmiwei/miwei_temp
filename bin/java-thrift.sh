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

# This script invokes java with class path for thrift clients

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh

$HYPERTABLE_HOME/bin/set-hadoop-distro.sh cdh4

javalib=$HYPERTABLE_HOME/lib/java
CLASSPATH=`echo $javalib/libthrift-*.jar`
for j in $javalib/hypertable*.jar $javalib/slf4j*.jar $javalib/log4j*.jar
do CLASSPATH=$CLASSPATH:$j
done

exec java -cp "$CLASSPATH" "$@"
