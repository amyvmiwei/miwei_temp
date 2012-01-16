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

# Post install script to FHSize the running layout according FHS
# currently FHS 2.3: http://www.pathname.com/fhs/pub/fhs-2.3.html

export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
version=`basename $HYPERTABLE_HOME`
varhome=/var/opt/hypertable
etchome=/etc/opt/hypertable

convert_var_directory() {
  subdir=$1

  touch $varhome/$subdir/foo
  if [ $? != 0 ]; then
    echo "error: Unable to write in directory '$varhome/$subdir'"
    exit 1
  fi
  /bin/rm -f $varhome/$subdir/foo

  source_link=`readlink $HYPERTABLE_HOME/$subdir`
  if [[ ${source_link:0:1} != "/" ]] ; then 
    source_link=`dirname $HYPERTABLE_HOME/$subdir`/$source_link
  fi

  if [ ! -e $HYPERTABLE_HOME/$subdir ] ; then
    ln -s $varhome/$subdir $HYPERTABLE_HOME
  elif [ $source_link != $varhome/$subdir ] ; then
    source_n=`ls $HYPERTABLE_HOME/$subdir/ | wc -l`
    dest_n=`ls $varhome/$subdir/ | wc -l`

    if [ $source_n -gt 0 ] && [ $dest_n -gt 0 ] ; then
      echo "Unable to FHSize, both $HYPERTABLE_HOME/$subdir/ and $varhome/$subdir/ are non-empty"
      exit 1
    elif [ $source_n -gt 0 ] ; then
      cp -r $HYPERTABLE_HOME/$subdir/* $varhome/$subdir/
    fi

    rm -rf $HYPERTABLE_HOME/$subdir
    ln -s $varhome/$subdir $HYPERTABLE_HOME
  fi

}

echo "Setting up $varhome"

mkdir -p $varhome/hyperspace $varhome/fs $varhome/run $varhome/log $etchome

convert_var_directory hyperspace
convert_var_directory fs
convert_var_directory run
convert_var_directory log

echo "Setting up $etchome"

touch $etchome/foo
if [ $? != 0 ]; then
  echo "error: Unable to write in directory '$etchome'"
  exit 1
fi
/bin/rm -f $etchome/foo

if [ ! -e $HYPERTABLE_HOME/conf ] ; then
  ln -s $etchome $HYPERTABLE_HOME/conf
else
  source_link=`readlink $HYPERTABLE_HOME/conf`
  if [[ ${source_link:0:1} != "/" ]] ; then 
    source_link=`dirname $HYPERTABLE_HOME/conf`/$source_link
  fi

  if [ $source_link != $etchome ] ; then
    nfiles=`ls -1 $etchome | wc -l`
    if [ $nfiles -eq 0 ] ; then
      cp $HYPERTABLE_HOME/conf/* $etchome
    else
      cp $HYPERTABLE_HOME/conf/METADATA.xml $etchome
      cp $HYPERTABLE_HOME/conf/RS_METRICS.xml $etchome
    fi
    rm -rf $HYPERTABLE_HOME/conf && ln -s $etchome $HYPERTABLE_HOME/conf
  fi
fi

echo "fshize $HYPERTABLE_HOME:  success"
