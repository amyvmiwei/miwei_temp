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

# exit on error
set -o errexit
trap 'echo "$status_msg"' EXIT INT TERM
status_msg="UPGRADE FAILED! Trace follows: "

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh
etchome=/etc/opt/hypertable
varhome=/var/opt/hypertable
verbose=

usage() {
  echo
  echo "usage: ht-upgrade.sh [OPTIONS] <old-install-dir>"
  echo
  echo "OPTIONS:"
  echo "  -h,--help     Display usage information"
  echo "  -v,--verbose  Display verbose output"
  echo
  echo "Upgrades the Hypertable installation on localhost.  The ht-upgrade.sh script"
  echo "should be run from the new installation and takes a single argument that is the"
  echo "absolute path of the top-level directory of the old installation.  For example,"
  echo "if upgrading from Hypertable version 0.9.8.6 to 0.9.8.7, you would issue the"
  echo "following command to perform the upgrade:"
  echo
  echo "  /opt/hypertable/0.9.8.7/bin/ht-upgrade.sh /opt/hypertable/0.9.8.6"
  echo
  echo "If the \"current\" link points to the old installation directory, the upgrade can"
  echo "alternatively be performed with:"
  echo
  echo "  /opt/hypertable/0.9.8.7/bin/ht-upgrade.sh /opt/hypertable/current"
  echo
  echo "This script updates the following subdirectories in the new installation:"
  echo
  echo "  conf"
  echo "  run"
  echo "  fs"
  echo "  hyperspace"
  echo "  log"
  echo
  echo "Each subdirectory is set up independently in the new installation directory"
  echo "according to the following three rules:"
  echo
  echo "1. Subdirectory exists in the old installation and is a symbolic link"
  echo
  echo "  The symbolic link is recreated in the new installation exactly as it exists in"
  echo "  the old installation."
  echo
  echo "2. Subdirectory exists in the old installation and is not a symbolic link"
  echo
  echo "  The subdirectory is recursively copied from the old installation to the new"
  echo "  installation."
  echo
  echo "3. Subdirectory does not exist in the old installation"
  echo
  echo "  A symbolic link is setup for the subdirectory in the new installation"
  echo "  directory as shown below.  The link will only be created if the destination"
  echo "  directory exists."
  echo
  echo "  conf -> /etc/opt/hypertable"
  echo "  run -> /var/opt/hypertable/run"
  echo "  fs -> /var/opt/hypertable/fs"
  echo "  hyperspace -> /var/opt/hypertable/hyperspace"
  echo "  log -> /var/opt/hypertable/log"
  echo
  echo "The conf subdirectory is handled slightly differently to allow certain files to"
  echo "be upgraded.  During the upgrade of the conf directory, the following files are"
  echo "copied from new installation into the resulting conf directory, thereby picking"
  echo "up any changes made to them in the new release."
  echo
  echo "  cluster.def-EXAMPLE"
  echo "  METADATA.xml"
  echo "  RS_METRICS.xml"
  echo "  notification-hook.tmpl"
  echo "  *.tasks"
  status_msg=
}

canonicalize() {
  local CANONICAL=$1
  readlink $1 > /dev/null
  if [ $? -eq 0 ]; then
    local LINK=`readlink $1`
    if [[ $LINK == "/"* ]]; then
      CANONICAL=$LINK
    else
      CANONICAL="`dirname $1`/$LINK"
    fi
  fi
  pushd . > /dev/null
  cd `dirname $CANONICAL`
  CANONICAL="`pwd`/`basename $CANONICAL`"
  popd > /dev/null
  echo $CANONICAL
}

run_command() {
  $@
  if [ -n "$verbose" ]; then
    echo "$@"
  fi
}

upgrade_dir() {
  dir=$1
  fhs_default_target=$2
  link_only=$3
  target=0

  old_dir=${FROM}/$dir
  new_dir=${TO}/$dir

  status_msg="$status_msg [UPGRADE '$dir' STARTED]"
  if [ -e ${old_dir} ] ; then
    if [ -L ${old_dir} ] ; then
      #setup symlink
      ls_out=$(ls -l "${old_dir}")
      target=${ls_out#*-> }
      status_msg="$status_msg :: '${old_dir}' symlinked to '${target}'. Setting up '${new_dir}' to link to '${target}'"
      if [ $dir == "conf" ] ; then
        run_command cp $new_dir/cluster.def-EXAMPLE $target
        run_command cp $new_dir/METADATA.xml $target
        run_command cp $new_dir/RS_METRICS.xml $target
        run_command cp $new_dir/notification-hook.tmpl $target
        run_command cp $new_dir/*.tasks $target
      fi
      run_command rm -rf $new_dir
      run_command ln -s $target $new_dir
    else 
      if [ $link_only -eq 0 ] ; then
        #copy stuff over
        status_msg="$status_msg :: '$old_dir' is not a symlink. Copying contents into '$new_dir'."
        target="$new_dir.tmp"
        run_command cp -dpR $old_dir $target
        if [ $dir == "conf" ] ; then
          run_command cp $new_dir/cluster.def-EXAMPLE $target
          run_command cp $new_dir/METADATA.xml $target 
          run_command cp $new_dir/RS_METRICS.xml $target
          run_command cp $new_dir/notification-hook.tmpl $target
          run_command cp $new_dir/*.tasks $target
        fi
        run_command rm -rf $new_dir 
        run_command mv $target $new_dir
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
        run_command cp $new_dir/cluster.def-EXAMPLE $target
        run_command cp $new_dir/METADATA.xml $target
        run_command cp $new_dir/RS_METRICS.xml $target
        run_command cp $new_dir/notification-hook.tmpl $target
        run_command cp $new_dir/*.tasks $target
      fi
      run_command rm -rf $new_dir 
      run_command ln -s $target $new_dir
    else
      status_msg="$status_msg :: $fhs_default_target' doesn't exist. Nothing to upgrade."
    fi
  fi
  status_msg="$status_msg [UPGRADE '$dir' FINISHED]"
}


while [ $# -gt 0 ]; do
  case $1 in
    -h|--help)
      usage;
      exit 0
      ;;
    -v|--verbose)
      verbose=true
      shift
      ;;
    *)
      break
      ;;
  esac
done

if [ $# -eq 0 ] ; then
  usage
  exit 0
fi

FROM=`canonicalize $1`
TO=`canonicalize $HYPERTABLE_HOME`

if [ -n "$verbose" ]; then
  echo "FROM = $FROM"
  echo "TO   = $TO"
fi

if [ "$FROM" == "$TO" ]; then
  echo "Previous version is same as the new one ... nothing to do."
  status_msg="Upgrade succeeded."
  exit 0
fi

upgrade_dir conf $etchome 0
upgrade_dir run  $varhome/run 0
upgrade_dir fs   $varhome/fs 0
upgrade_dir hyperspace  $varhome/hyperspace 0
upgrade_dir log  $varhome/log 1
status_msg="Upgrade succeeded."
