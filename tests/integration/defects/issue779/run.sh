#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #779"
echo "======================="

check ()
{
  from=$1
  to=$2
  expected=$3
  echo "$from -> $to"
  $HT_HOME/bin/upgrade-ok.sh $from $to
  if [ "$?" -ne "$expected" ]
  then
    echo "return code mismatch"
    exit 1
  fi
}

# positive tests
check 1.1.1.1 1.1.1.2 0
check 1.0.0.0 1.0.1.0 0
check 0.9.6.0 1.0.0.0 0
check 0.9.5.0 1.0.0.0 0
check 0.9.5.0 0.9.6.0 0
check 0.9.4.0 1.0.0.0 0

check 0.9.6.0 0.9.5.2 0

# negative tests
check 1.1.1.2 1.1.1.1 1
check 1.0.1.1 1.0.1.0 1
check 1.0.0.0 0.9.6.0 1
check 1.0.0.0 0.9.5.5 1
check 1.0.0.0 0.9.4.1 1

# issue 850
check 0.9.5.6/ 0.9.5.7 0
check 0.9.5.6/ 0.9.5.6 1

echo "SUCCESS"
