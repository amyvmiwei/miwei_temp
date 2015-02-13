#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/ht-start-test-servers.sh --clear \
    --Hypertable.Master.Gc.Interval=30000 \
    --Hypertable.RangeServer.Range.SplitSize=33K \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
    --Hypertable.Mutator.FlushDelay=250 \
    --Hypertable.RangeServer.Scanner.BufferSize=2000

echo "CREATE TABLE LargeRowTest ( c );" | $HT_HOME/bin/ht shell --batch

$SCRIPT_DIR/populate.py 500000

echo "SELECT * FROM LargeRowTest LIMIT 1;" | $HT_HOME/bin/ht shell --batch > large-row.output

diff $SCRIPT_DIR/large-row.golden large-row.output
if [ $? -ne 0 ]; then
    echo "Output differs from golden output"
    exit 1
fi

exit 0
