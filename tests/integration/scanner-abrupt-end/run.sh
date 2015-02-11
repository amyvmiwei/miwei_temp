#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
SPEC_FILE="$SCRIPT_DIR/../../data/random-test.spec"
ITERATIONS=5
TEST_BIN=./scanner_abrupt_end_test
MAX_KEYS=50000

set -v

$HT_HOME/bin/ht-start-test-servers.sh --clear

cmd="$HT_HOME/bin/ht hypertable --no-prompt --command-file=$SCRIPT_DIR/create-table.hql"
echo "$cmd"
${cmd}

echo "================="
echo "random WRITE test"
echo "================="
cmd="$HT_HOME/bin/ht load_generator update --spec-file=$SPEC_FILE --max-keys=$MAX_KEYS --rowkey-seed=44"
echo "$cmd"
${cmd}

cd ${TEST_BIN_DIR};
let step=MAX_KEYS/ITERATIONS
num_cells=0
\rm -f scanner_abrupt_end_test.out
for((ii=0; $ii<${ITERATIONS}; ii=$ii+1)) do
  let num_cells=${num_cells}+${step}
  cmd="${TEST_BIN} ${num_cells}"
  echo "Running '${cmd}'"
  `${cmd}  2>&1 >> scanner_abrupt_end_test.out`
  if [ $? -ne 0 ] ; then
    echo "${cmd} failed got error %?" 
    exit 1
  fi
done

exit 0
