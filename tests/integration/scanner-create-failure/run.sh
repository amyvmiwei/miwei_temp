#!/usr/bin/env bash

SCRIPT_DIR=`dirname $0`
HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SHELL_OPTIONS="--no-prompt --batch --Hypertable.Request.Timeout=15000"

set -v

$HT_HOME/bin/start-test-servers.sh --clean --"induce-failure=create-scanner-1:throw:15"

cmd="${HT_HOME}/bin/ht hypertable"
${cmd} ${SHELL_OPTIONS} < ${SCRIPT_DIR}/run.hql > test.out

$HT_HOME/bin/start-test-servers.sh --clean --"induce-failure=create-scanner-1:exit:15"

${cmd} ${SHELL_OPTIONS} < ${SCRIPT_DIR}/run.hql >> test.out

$HT_HOME/bin/start-test-servers.sh --clean --"induce-failure=create-scanner-1:exit:16"

${cmd} ${SHELL_OPTIONS} < ${SCRIPT_DIR}/run.hql >> test.out

exit 0
