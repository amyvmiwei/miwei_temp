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

usage() {
  local REAL_HOME=$HYPERTABLE_HOME
  readlink $HYPERTABLE_HOME > /dev/null
  if [ $? -eq 0 ]; then
    REAL_HOME="`dirname $HYPERTABLE_HOME`/`readlink $HYPERTABLE_HOME`"
  fi
  echo
  echo "usage: ht-fhsize.sh [OPTIONS]"
  echo
  echo "OPTIONS:"
  echo "  -h,--help             Display usage information"
  echo
  echo "This script FHS'izes the Hypertable installation.  This involves setting the"
  echo "hyperspace, fs, conf, run, and log directories to point to standard system"
  echo "directories, which are preserved across upgrades.  FHS stands for Filesystem"
  echo "Hierarchy Standard (see http://www.pathname.com/fhs/ for details)."
  echo
  echo "The script starts by creating the following directories if they don't"
  echo "already exist:"
  echo
  echo "  $varhome/hyperspace"
  echo "  $varhome/fs"
  echo "  $varhome/run"
  echo "  $varhome/log"
  echo "  $etchome"
  echo
  echo "Then for each of the hyperspace, fs, run, and log subdirectories, it makes"
  echo "sure that at least one of the subdirectories within $REAL_HOME"
  echo "or $varhome is empty.  If both directories are non-empty, the script"
  echo "will print an error message to the terminal and exit with status 1.  Otherwise,"
  echo "if the subdirectory within $REAL_HOME is non-empty, the"
  echo "contents are copied to the corresponding subdirectory within $varhome."
  echo "Lastly, the subdirectory within $REAL_HOME is recursively"
  echo "removed and a symbolic link is setup to point to the subdirectory within"
  echo "$varhome."
  echo
  echo "The conf directory is handled slightly differently so that certain files"
  echo "in the new installation (e.g. core.tasks) get copied into the system directory."
  echo "If the $etchome directory is empty, then the entire contents of the"
  echo "$REAL_HOME/conf directory are copied into $etchome."
  echo "If both the $etchome and $REAL_HOME/conf directories"
  echo "are non-empty, then all of the files in $REAL_HOME/conf that"
  echo "don't match the patterns *.cfg, *.def, and notification-script.sh are copied"
  echo "into $etchome.  Lastly, the $REAL_HOME/conf is"
  echo "recursively removed and a symbolic link $REAL_HOME/conf is"
  echo "created pointing to $etchome."
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
      pushd .
      rm -f /tmp/hypertable-conf.tgz
      cd $HYPERTABLE_HOME/conf/
      tar czf /tmp/hypertable-conf.tgz --exclude=*.cfg --exclude=*.def --exclude=notification-hook.sh *
      cd $etchome
      tar xzf /tmp/hypertable-conf.tgz
      rm -f /tmp/hypertable-conf.tgz
      popd
    fi
    rm -rf $HYPERTABLE_HOME/conf && ln -s $etchome $HYPERTABLE_HOME/conf
  fi
fi

echo "fshize $HYPERTABLE_HOME:  success"
