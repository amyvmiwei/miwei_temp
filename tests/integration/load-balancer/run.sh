#!/usr/bin/env bash

set -v

SCRIPT_DIR=`dirname $0`
HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
TEST_BIN=./load_balancer_test
TEST_OUTPUT_FILE="load_balancer_test.out"
BALANCE_OUTPUT_FILE="balance_plans.out"
BALANCE_GOLDEN_FILE="${SCRIPT_DIR}/balance_plans.golden"
TEST_RS_METRICS_FILE="${SCRIPT_DIR}/rs_metrics.txt"

$HT_HOME/bin/start-test-servers.sh --clean

if [ -e $BALANCE_OUTPUT_FILE ]; then
  rm $BALANCE_OUTPUT_FILE 
fi

set -e

cmd="$HT_HOME/bin/ht ht_balance_plan_generator --Hypertable.LoadBalancer.LoadavgThreshold=.001 --balance-plan-file=${BALANCE_OUTPUT_FILE} --rs-metrics-dump ${TEST_RS_METRICS_FILE}"
echo "$cmd"
${cmd}

cmd="$HT_HOME/bin/ht ht_balance_plan_generator --Hypertable.LoadBalancer.LoadavgThreshold=.10 --balance-plan-file=${BALANCE_OUTPUT_FILE} --rs-metrics-loaded"
echo "$cmd"
${cmd}

cmd="$HT_HOME/bin/ht ht_balance_plan_generator --Hypertable.LoadBalancer.LoadavgThreshold=.25 --balance-plan-file=${BALANCE_OUTPUT_FILE} --rs-metrics-loaded"
echo "$cmd"
${cmd}

cmd="$HT_HOME/bin/ht ht_balance_plan_generator --Hypertable.LoadBalancer.LoadavgThreshold=1 --balance-plan-file=${BALANCE_OUTPUT_FILE} --rs-metrics-loaded"
echo "$cmd"
${cmd}

cmd="diff ${BALANCE_OUTPUT_FILE} ${BALANCE_GOLDEN_FILE}"
echo "$cmd"
${cmd}

exit 0
