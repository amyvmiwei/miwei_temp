#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
HQL_CREATE=$1
LOAD_GEN_FLAG=$2

. $HT_HOME/bin/ht-env.sh

$HT_HOME/bin/start-test-servers.sh --no-thriftbroker --clear \
    --Hypertable.RangeServer.Range.SplitSize=10000000 \
    --Hypertable.RangeServer.AccessGroup.GarbageThreshold.Percentage=20 \
    --Hypertable.RangeServer.Maintenance.Interval=100 \
    --Hypertable.RangeServer.Timer.Interval=100 \
    --Hypertable.RangeServer.AccessGroup.MaxMemory=250000

sleep 3

$HT_SHELL --batch --no-prompt < $SCRIPT_DIR/$HQL_CREATE

$HT_HOME/bin/ht ht_load_generator update \
    --spec-file=${SCRIPT_DIR}/data.spec \
    --table=IndexTest --max-keys=30000 \
    --row-seed=1 --seed=1 $LOAD_GEN_FLAG \
    --delete-percentage=30

# wait a few seconds to make sure that the TTL=1 test works
sleep 5

# trigger the compaction
$HT_HOME/bin/ht_rsclient --exec "COMPACT RANGES USER; WAIT FOR MAINTENANCE;"
sleep 10

echo "Verifying Field1"

# dump the column Field1 into a file and verify that all keys are indexed
$HT_SHELL --batch --test-mode --namespace / --exec "SELECT Field1 FROM IndexTest DISPLAY_TIMESTAMPS;" > Field1.output
perl ${SCRIPT_DIR}/verify.pl Field1.output 0 > Field1.hql
if [ $? -ne "0" ]
then
  echo "(1) Verification of Field1 failed"
  exit -1
fi
$HT_SHELL --batch --test-mode --namespace / --command-file Field1.hql > Field1.results
if [ $? -ne "0" ]
then
  echo "(2) Verification of Field1 failed"
  exit -1
fi

# make sure that the keys that are fetched through the index are identical with
# those from the original query
diff Field1.results Field1.output
if [ $? -ne "0" ]
then
  echo "(3) Verification of Field1 failed"
  exit -1
fi


echo "Verifying Field2"

# dump the column Field2 into a file and verify that all qualifiers
# are indexed
$HT_SHELL --batch --test-mode --namespace / --exec "SELECT Field2 FROM IndexTest DISPLAY_TIMESTAMPS;" > Field2.output
perl ${SCRIPT_DIR}/verify.pl Field2.output 1 > Field2.hql
if [ $? -ne "0" ]
then
  echo "(1) Verification of Field2 failed"
  exit -1
fi
$HT_SHELL --batch --test-mode --namespace / --command-file Field2.hql > Field2.results
if [ $? -ne "0" ]
then
  echo "(2) Verification of Field2 failed"
  exit -1
fi

# make sure that the keys that are fetched through the index are identical with
# those from the original query
diff Field2.results Field2.output
if [ $? -ne "0" ]
then
  echo "(3) Verification of Field2 failed"
  exit -1
fi

exit 0

