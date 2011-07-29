#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
SCRIPT_OUTPUT=hyperspace_dump.out

$HT_HOME/bin/start-test-servers.sh --clear

echo "[test1]" > ${SCRIPT_OUTPUT}
$HT_HOME/bin/ht hyperspace --command-file hyperspace_dump_test1.in
echo "dump /dumptest AS_COMMANDS 'hyperspace_dump_test1.out';" | $HT_HOME/bin/ht hyperspace --no-prompt
if cmp hyperspace_dump_test1.out hyperspace_dump_test1.golden &> /dev/null 
then echo "[test1] passed" >> ${SCRIPT_OUTPUT}
else exit 1
fi

echo "[test2]" >> ${SCRIPT_OUTPUT}
$HT_HOME/bin/ht hyperspace --command-file hyperspace_dump_test2.in
echo "dump /dumptest AS_COMMANDS 'hyperspace_dump_test2.out';" | $HT_HOME/bin/ht hyperspace --no-prompt
if cmp hyperspace_dump_test2.out hyperspace_dump_test2.golden &> /dev/null 
then echo "[test2] passed" >> ${SCRIPT_OUTPUT}
else exit 1
fi
