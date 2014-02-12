#!/usr/bin/env bash
#
# Copyright (C) 2007-2012 Hypertable, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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

stop_monitoring() {
  cd ${monitoring_script_dir}
  if [ -e ${pidfile} ]; then
    ps -fp `cat ${pidfile}` > /dev/null
    if [ $? -eq 0 ]; then
      command="thin -p ${port} -e production -P ${pidfile} -l ${log} -R ${rack_file} -d stop"
      $command
    fi
  fi
}

