#!/usr/bin/env bash

SCRIPT_DIR=`dirname $0`
HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
TEST_BIN=./future_mutator_cancel_test

KEY_SIZE=10
NUM_MUTATORS_INIT=5
CELLS_PER_FLUSH_INIT=1000
ITERATIONS=5
APPROX_CELL_SIZE=30
TOTAL_CELLS=150000

set -v

cmd="$HT_HOME/bin/ht hypertable --no-prompt --command-file=$SCRIPT_DIR/create-table.hql"
echo "================="
echo "Running '${cmd}'"
echo "================="
${cmd}

cd ${TEST_BIN_DIR};

num_cells=0
rm future_mutator_cancel_test.out
for((ii=0; $ii<${ITERATIONS}; ii=$ii+1)) do
  let num_mutators=${NUM_MUTATORS_INIT}+${ii}
  let cells_per_flush=${CELLS_PER_FLUSH_INIT}+${ii}*100
  cmd="${TEST_BIN} ${num_mutators} ${cells_per_flush} ${TOTAL_CELLS}"
  echo "================="
  echo "Running '${cmd}'"
  echo "================="
  `${cmd}  2>&1 >> future_mutator_cancel_test.out`
  if [ $? -ne 0 ] ; then
    echo "${cmd} failed got error $?" 
    exit 1
  fi
done

exit 0
