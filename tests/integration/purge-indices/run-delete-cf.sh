#!/usr/bin/env bash

echo "========================================================================"
echo "Purge indices tests 3 (FLAG_DELETE_COLUMN_FAMILY)"
echo "========================================================================"

SCRIPT_DIR=`dirname $0`

$SCRIPT_DIR/_run.sh create-table.hql --overwrite-delete-flag=DELETE_COLUMN_FAMILY

exit $?

