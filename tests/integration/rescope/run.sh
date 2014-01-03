#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

function finish {
    $HT_HOME/bin/clean-database.sh
}
trap finish EXIT

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
    --Hypertable.RangeServer.Range.SplitSize=100000 \
    --Hypertable.Master.Split.SoftLimitEnabled=false \
    --Hypertable.RangeServer.Maintenance.Interval 1000 \
    --Hypertable.Request.Timeout=60000
$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

echo "Loading IN_MEMORY Field1 ..."
$HT_HOME/bin/ht load_generator update --spec-file=$SCRIPT_DIR/data1.spec --row-seed=1 --max-bytes=150000
echo "COMPACT RANGES ALL;" | $HT_HOME/bin/ht shell --batch
sleep 4

for ((seed=2; seed<=10; seed++)); do
    echo "Loading Field2 (seed=$seed) ..."
    $HT_HOME/bin/ht load_generator update --spec-file=$SCRIPT_DIR/data2.spec --row-seed=$seed --max-bytes=200000
    if [ $? -ne 0 ]; then
        echo "Load failed."
        exit 1
    fi
    sleep 1
done

exit 0
