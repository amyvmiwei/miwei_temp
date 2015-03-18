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

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)

port=15860
pidfile="$HYPERTABLE_HOME/run/MonitoringServer.pid"
log="$HYPERTABLE_HOME/log/MonitoringServer.log"
monitoring_script_dir="$HYPERTABLE_HOME/Monitoring"
config_file="${monitoring_script_dir}/app/config/config.yml"
rack_file="${monitoring_script_dir}/config.ru"

start_monitoring() {
  cd ${monitoring_script_dir}
  command="thin -p ${port} -e production -P ${pidfile} -l ${log}  -R ${rack_file} -d start"
  $command 
}

usage() {
  local REAL_HOME=$HYPERTABLE_HOME
  readlink $HYPERTABLE_HOME > /dev/null
  if [ $? -eq 0 ]; then
    REAL_HOME="`dirname $HYPERTABLE_HOME`/`readlink $HYPERTABLE_HOME`"
  fi
  pidfile="$REAL_HOME/run/MonitoringServer.pid"
  log="$REAL_HOME/log/MonitoringServer.log"
  monitoring_script_dir="$REAL_HOME/Monitoring"
  config_file="${monitoring_script_dir}/app/config/config.yml"
  rack_file="${monitoring_script_dir}/config.ru"
  echo
  echo "usage: ht-start-monitoring.sh [OPTIONS]"
  echo
  echo "OPTIONS:"
  echo "  -h,--help  Display usage information"
  echo
  echo "Starts the monitoring web server.  This script starts the monitoring"
  echo "web server process (thin) as follows:"
  echo
  echo "cd ${monitoring_script_dir}"
  echo
  echo "thin -p ${port} \\"
  echo "     -e production \\"
  echo "     -P ${pidfile} \\"
  echo "     -l ${log} \\"
  echo "     -R config.ru \\"
  echo "     -d start"
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

start_monitoring 
