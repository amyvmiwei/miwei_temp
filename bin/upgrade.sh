#!/usr/bin/env bash
#
# Copyright (C) 2007-2012 Hypertable, Inc.
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

# exit on error
set -o errexit
trap 'echo "$status_msg"' EXIT INT TERM
status_msg="UPGRADE FAILED! Trace follows: "

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh
etchome=/etc/opt/hypertable
varhome=/var/opt/hypertable

usage() {
  echo ""
  echo "usage: $0 <from> <to>"
  echo ""
  echo "description:"
  echo ""
  echo "  Upgrades the <dir> directory in <from> to <to>"
  echo "  Copies files or uses symlinks from <from> to <to>."
  echo ""
  echo ""
}

upgrade_dir() {
  dir=$1
  fhs_default_target=$2
  link_only=$3
  target=0
  
  cwd=`pwd`
  old_dir=${HYPERTABLE_HOME}/../${FROM}/$dir
  new_dir=${HYPERTABLE_HOME}/$dir
  
  status_msg="$status_msg [UPGRADE '$dir' STARTED]"
  if [ -e ${old_dir} ] ; then
    if [ -L ${old_dir} ] ; then
      #setup symlink
      ls_out=$(ls -l "${old_dir}")
      target=${ls_out#*-> }
      status_msg="$status_msg :: '${old_dir}' symlinked to '${target}'. Setting up '${new_dir}' to link to '${target}'"
      if [ $dir == "conf" ] ; then
        cp $new_dir/METADATA.xml $target  
        cp $new_dir/RS_METRICS.xml $target
        cp $new_dir/notification-hook.tmpl $target
      fi
      cd $HYPERTABLE_HOME 
      rm -rf $dir 
      ln -s $target $dir
    else 
      if [ $link_only -eq 0 ] ; then
        #copy stuff over
        status_msg="$status_msg :: '$old_dir' is not a symlink. Copying contents into '$new_dir'."
        target="$new_dir.tmp"
        cp -dpR $old_dir $target
        if [ $dir == "conf" ] ; then
          cp $new_dir/METADATA.xml $target 
          cp $new_dir/RS_METRICS.xml $target
          cp $new_dir/notification-hook.tmpl $target
        fi
        cd $HYPERTABLE_HOME 
        rm -rf $new_dir 
        mv $target $new_dir
      else
        status_msg="$status_msg :: '$old_dir' is not a symlink. Skipping content copy into $new_dir."
      fi
    fi
  else
    #check for stuff under default_link_target
    status_msg="$status_msg :: '$old_dir' doesn't exist. Checking default location '$fhs_default_target.'"
    if [ -e $fhs_default_target ] ; then
       #setup symlink
      target=$fhs_default_target
      status_msg="$status_msg :: '${fhs_default_target}' found. Setting up '${new_dir}' to link to '${target}'"
      if [ $dir == "conf" ] ; then
        cp $new_dir/METADATA.xml $target 
        cp $new_dir/RS_METRICS.xml $target
      fi
      cd $HYPERTABLE_HOME 
      rm -rf $dir 
      ln -s $target $dir
    else
      status_msg="$status_msg :: $fhs_default_target' doesn't exist. Nothing to upgrade."
    fi
  fi
  cd $cwd
  status_msg="$status_msg [UPGRADE '$dir' FINISHED]"
}

if [ $# != 2 ] ; then
  usage
  return 0
fi
FROM=`echo $1 | awk -F'/' '{ print $NF; }'`
TO=`echo $2 | awk -F'/' '{ print $NF; }'`

if [ "$FROM" == "current" ] ; then
  FROM=`/bin/ls -l $1 | tr -s " " | awk '{ print $NF; }' | awk -F'/' '{ print $NF; }'`
fi

if [ "$TO" == "current" ] ; then
  TO=`/bin/ls -l $2 | tr -s " " | awk '{ print $NF; }' | awk -F'/' '{ print $NF; }'`
fi

upgrade_dir conf $etchome 0
upgrade_dir run  $varhome/run 0
upgrade_dir fs   $varhome/fs 0
upgrade_dir hyperspace  $varhome/hyperspace 0
upgrade_dir log  $varhome/log 1
status_msg="Upgrade succeeded"
