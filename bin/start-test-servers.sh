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

INSTALL_DIR=${INSTALL_DIR:-$(cd `dirname $0`/.. && pwd)}
HT_TEST_FS=${HT_TEST_FS:-local}

usage_exit() {
  echo "$0 [Options]"
  echo ""
  echo "Options:"
  echo "  --clear               Clear any existing data"
  echo "  -h, --help            Show this help message"
  echo "  and any valid start-all-servers.sh options"
}

while [ $# -gt 0 ]; do
  case $1 in
    --clear)              clear=1;;
    --clean)              clear=1;;
    -h|--help)            usage_exit;;
    --val*|--no*|--heap*) opts[${#opts[*]}]=$1;;
    *)                    break;;
  esac
  shift
done

if [ "$clear" ]; then
  $INSTALL_DIR/bin/start-fsbroker.sh $HT_TEST_FS
  $INSTALL_DIR/bin/ht clean-database $@
else
  $INSTALL_DIR/bin/ht stop servers
fi

$INSTALL_DIR/bin/ht start all-servers "${opts[@]}" $HT_TEST_FS $@
