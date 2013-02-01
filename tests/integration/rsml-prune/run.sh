#!/usr/bin/env bash

HYPERTABLE_HOME=/opt/hypertable/current
HT_TEST_DFS=${HT_TEST_DFS:-local}

PIDFILE=$HYPERTABLE_HOME/run/Hypertable.RangeServer.pid

$HYPERTABLE_HOME/bin/start-test-servers.sh --clean --no-thriftbroker

sleep 5

echo "create table LoadTest ( Field );" | $HYPERTABLE_HOME/bin/hypertable --batch

if [ $? != 0 ] ; then
    echo "Unable to create table 'LoadTest', exiting ..."
    exit 1
fi

$HYPERTABLE_HOME/bin/ht_load_generator update --spec-file=data.spec --max-bytes=500K

for ((i=0; i<15; i++)) ; do
    kill -9 `cat $PIDFILE`
    \rm -f $PIDFILE
    $HYPERTABLE_HOME/bin/start-rangeserver.sh
    sleep 2
done

ls -1 $HYPERTABLE_HOME/fs/local/hypertable/servers/*/log/range_txn | sort -n > rsml-dir-listing.out

diff rsml-dir-listing.out rsml-dir-listing.golden
if [ $? != 0 ] ; then
    echo "Test failed, exiting ..."
    exit 1
fi
echo "Test passed."

exit 0
