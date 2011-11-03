#!/usr/bin/env bash

echo "========================================================================"
echo "Purge indices tests 4 (MAX_VERSIONS=1)"
echo "========================================================================"

SCRIPT_DIR=`dirname $0`

$SCRIPT_DIR/_run.sh create-table-maxv.hql

exit $?
