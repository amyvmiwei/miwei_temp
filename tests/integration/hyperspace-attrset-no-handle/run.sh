#!/bin/bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
SCRIPT_OUTPUT=hyperspace_dump.out

$HT_HOME/bin/ht-destroy-database.sh

$HT_HOME/bin/ht-start-hyperspace.sh

echo "create /foo flags=READ|WRITE; quit;" | $HT_HOME/bin/ht hyperspace --batch

$HT_HOME/bin/ht-stop-hyperspace.sh

$HT_HOME/bin/ht-start-hyperspace.sh

echo "attrset /foo bar=x; quit;" | $HT_HOME/bin/ht hyperspace --batch

if [ $? -ne 0 ]; then
  echo "Problem setting attribute 'bar' of file '/foo'";
  exit 1;
fi

