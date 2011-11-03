#!/usr/bin/env bash

echo "========================================================================"
echo "Purge indices tests 2 (FLAG_DELETE_ROW)"
echo "========================================================================"

SCRIPT_DIR=`dirname $0`

$SCRIPT_DIR/_run.sh create-table.hql --overwrite-delete-flag=DELETE_ROW

exit $?
