#!/usr/bin/env bash

echo "========================================================================"
echo "Purge indices tests 5 (TTL=1)"
echo "========================================================================"

SCRIPT_DIR=`dirname $0`

$SCRIPT_DIR/_run.sh create-table-ttl.hql

exit $?

