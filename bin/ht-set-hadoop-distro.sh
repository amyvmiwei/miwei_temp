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

export HT_HOME=$(cd `dirname "$0"`/.. && pwd)

declare -a Distros=('apache1' 'apache2' 'cdh3' 'cdh4' 'cdh5' 'hdp2');

usage() {
  local REAL_HOME=$HT_HOME
  readlink $HT_HOME > /dev/null
  if [ $? -eq 0 ]; then
    REAL_HOME="`dirname $HT_HOME`/`readlink $HT_HOME`"
  fi
  VERSION=`basename $REAL_HOME`
  THRIFT_JAR=`ls $REAL_HOME/lib/java/common/libthrift-*`
  THRIFT_JAR=`basename $THRIFT_JAR`
  echo
  echo "usage: ht-set-hadoop-distro.sh [OPTIONS] <distro>"
  echo
  echo "OPTIONS:"
  echo "  -h,--help  Display usage information"
  echo
  echo "This script will copy the jar files for Hadoop distribution specified by"
  echo "<distro> into the primary jar directory ($REAL_HOME/lib/java)."
  echo "This ensures that the correct Java programs (e.g. Hadoop FsBroker) and"
  echo "associated jar files will be used for the Hadoop distribution upon which"
  echo "Hypertable will be running.  The currently supported values for <distro>"
  echo "include:"
  echo
  for distro in "${Distros[@]}"; do
    echo "  $distro"
  done
  echo
  echo "It starts by clearing the primary jar directory with the following command:"
  echo
  echo "  rm -f $REAL_HOME/lib/java/*.jar"
  echo
  echo "Then it copies the jar files appropriate for the distribution from the"
  echo "lib/java/common, lib/java/specific, lib/java/<distro>, lib/java/apache1,"
  echo "and/or lib/java/apache2 installation subdirectories into the primary jar"
  echo "directory.  Lastly, it sets up convenience links in the primary jar"
  echo "directory for the hypertable and thrift jar files.  For example:"
  echo
  echo "  hypertable-examples.jar -> hypertable-examples-$VERSION-apache2.jar"
  echo "  hypertable.jar -> hypertable-$VERSION-apache2.jar"
  echo "  libthrift.jar -> $THRIFT_JAR"
  echo
}

if [ "$#" -eq 0 ] || [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
  usage
  exit 0
fi

DISTRO=
for distro in ${Distros[@]}; do
    if [ $1 == $distro ]; then
        DISTRO=$1
        shift
        break;
    fi
done

# Check for valid distro
if [ -z $DISTRO ]; then
    usage
    echo "error: unrecognized distro '$1'"
    exit 1;
fi

\rm -f $HT_HOME/lib/java/*.jar
\cp -f $HT_HOME/lib/java/common/*.jar $HT_HOME/lib/java

if [ -d $HT_HOME/lib/java/$DISTRO ]; then
  \cp -f $HT_HOME/lib/java/$DISTRO/*.jar $HT_HOME/lib/java
fi

if [ $DISTRO == "cdh4" ] || [ $DISTRO == "ibmbi3" ]; then
    \cp $HT_HOME/lib/java/apache2/hypertable-*.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/guava-11.0.2.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/protobuf-java-2.4.0a.jar $HT_HOME/lib/java
elif [ $DISTRO == "cdh5" ]; then
    \cp $HT_HOME/lib/java/apache2/*.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/guava-11.0.2.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/protobuf-java-2.5.0.jar $HT_HOME/lib/java
elif [ $DISTRO == "hdp2" ]; then
    \cp $HT_HOME/lib/java/apache2/*.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/guava-11.0.2.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/protobuf-java-2.5.0.jar $HT_HOME/lib/java
elif [ $DISTRO == "apache2" ]; then
    \cp $HT_HOME/lib/java/specific/guava-11.0.2.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/protobuf-java-2.5.0.jar $HT_HOME/lib/java
fi

# Setup symlinks
pushd .
cd $HT_HOME/lib/java
\ln -sf hypertable-[0-9]* hypertable.jar
\ln -sf hypertable-examples-* hypertable-examples.jar
\ln -sf libthrift-* libthrift.jar
popd

echo $DISTRO > $HT_HOME/conf/hadoop-distro

echo "Hypertable successfully configured for Hadoop $DISTRO"

exit 0
