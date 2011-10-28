#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

cp ${SCRIPT_DIR}/test.tsv .

echo "======================="
echo "Defect #720"
echo "======================="

cat ${SCRIPT_DIR}/test.hql | $HT_HOME/bin/ht hypertable --no-prompt --test-mode

# the first line in the outfile is a memory address which always changes,
# therefore remove it otherwise we cannot compare it against the golden file
cut -f2,3 test.output > test.out 

diff test.out ${SCRIPT_DIR}/test.golden
if [ $? -ne "0" ]
then
  echo "FAIL - golden file differs, exiting"
  exit 1
fi

echo "SUCCESS"
