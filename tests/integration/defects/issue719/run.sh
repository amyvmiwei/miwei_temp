#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

cp ${SCRIPT_DIR}/test.tsv .

echo "======================="
echo "Defect #719"
echo "======================="

cat ${SCRIPT_DIR}/test.hql | $HT_HOME/bin/ht hypertable \
    --no-prompt --test-mode \
    > test.output

diff test.output ${SCRIPT_DIR}/test.golden
if [ $? -ne "0" ]
then
  echo "FAIL - golden file differs, exiting"
  exit 1
fi

echo "SUCCESS"
