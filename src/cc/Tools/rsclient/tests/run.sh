#!/bin/bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/start-test-servers.sh --clean

echo "CREATE NAMESPACE test;" | $HT_HOME/bin/ht shell --test-mode
if [ $? -ne 0 ]; then
  echo "Problem creating 'test' namespace."
  exit 1
fi

$HT_HOME/bin/ht shell --test-mode < ${SCRIPT_DIR}/initialize.hql > init.out
if [ $? -ne 0 ]; then
  echo "Initialization failed."
  exit 1
fi

#
# Test1
#

$HT_HOME/bin/ht rsclient --test-mode localhost < ${SCRIPT_DIR}/Test1.cmd > Test1.output
if [ $? -ne 0 ]; then
  echo "Test1 failure."
  exit 1
fi

diff Test1.output ${SCRIPT_DIR}/Test1.golden
if [ $? -ne 0 ]; then
  echo "Test1 diff failure."
  exit 1
fi

#
# Test2
#

$HT_HOME/bin/ht rsclient --test-mode localhost < ${SCRIPT_DIR}/Test2.cmd > Test2.output
if [ $? -ne 0 ]; then
  echo "Test2 failure."
  exit 1
fi

diff Test2.output ${SCRIPT_DIR}/Test2.golden
if [ $? -ne 0 ]; then
  echo "Test2 diff failure."
  exit 1
fi

#
# Test3
#

$HT_HOME/bin/ht rsclient --test-mode localhost < ${SCRIPT_DIR}/Test3.cmd > Test3.output
if [ $? -ne 0 ]; then
  echo "Test3 failure."
  exit 1
fi

diff Test3.output ${SCRIPT_DIR}/Test3.golden
if [ $? -ne 0 ]; then
  echo "Test3 diff failure."
  exit 1
fi

#
# Test4
#

$HT_HOME/bin/ht rsclient --test-mode localhost < ${SCRIPT_DIR}/Test4.cmd > Test4.output
if [ $? -ne 0 ]; then
  echo "Test4 failure."
  exit 1
fi

diff Test4.output ${SCRIPT_DIR}/Test4.golden
if [ $? -ne 0 ]; then
  echo "Test4 diff failure."
  exit 1
fi

