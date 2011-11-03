#!/usr/bin/env bash

echo "========================================================================"
echo "Purge indices tests 1 (FLAG_DELETE_CELL)"
echo "========================================================================"

SCRIPT_DIR=`dirname $0`

$SCRIPT_DIR/_run.sh create-table.hql

exit $?

