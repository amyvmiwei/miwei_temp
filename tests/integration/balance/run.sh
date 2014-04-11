#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
NUM_POLLS=${NUM_POLLS:-"10"}
WRITE_SIZE=${WRITE_SIZE:-"40000000"}
RS1_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs1.pid
RS2_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs2.pid

save_failure_state() {
  ARCHIVE_DIR="archive-"`date | sed 's/ /-/g'`
  mkdir $ARCHIVE_DIR
  cp $HT_HOME/log/* $ARCHIVE_DIR
  cp ~/build/hypertable/debug/Testing/Temporary/LastTest.log.tmp $ARCHIVE_DIR
  cp -f core* $ARCHIVE_DIR
  cp rangeserver.* $ARCHIVE_DIR
  cp errors.txt *.output $ARCHIVE_DIR
  cp -r $HT_HOME/fs $ARCHIVE_DIR
  touch failure
}

kill_range_servers() {
    kill -9 `cat $RS1_PIDFILE`
    kill -9 `cat $RS2_PIDFILE`
    \rm -f $RS1_PIDFILE $RS2_PIDFILE
}

if [ -e failure ]; then
  kill -9 `ps auxww | fgrep -i "$HT_HOME/bin" | fgrep -v java | fgrep -v grep | tr -s "[ ]" | cut -f2 -d' '`
  sleep 10
  kill -9 `ps auxww | fgrep -i "$HT_HOME/bin" | fgrep -v java | fgrep -v grep | tr -s "[ ]" | cut -f2 -d' '`
  rm -f failure
fi

$HT_HOME/bin/start-test-servers.sh --clear --no-rangeserver --FsBroker.DisableFileRemoval=true

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs1 \
   --Hypertable.RangeServer.Port=15870 \
   --Hypertable.RangeServer.Maintenance.Interval 100 \
   --Hypertable.RangeServer.Range.SplitSize=400K 2>&1 > rangeserver.rs1.output&

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs2 \
   --Hypertable.RangeServer.Port=15871 \
   --Hypertable.RangeServer.Maintenance.Interval 100 \
   --Hypertable.RangeServer.Range.SplitSize=400K 2>&1 > rangeserver.rs2.output&

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht ht_load_generator update \
    --Hypertable.Mutator.FlushDelay=50 \
    --rowkey.component.0.order=random \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.min=0 \
    --rowkey.component.0.max=1000000 \
    --row-seed=1 \
    --Field.value.size=1000 \
    --max-bytes=$WRITE_SIZE

sleep 3

echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:15871
echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:15870

sleep 1

kill_range_servers

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs1 \
   --Hypertable.RangeServer.Port=15872 2>&1 >> rangeserver.rs1.output&

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs2 \
   --Hypertable.RangeServer.Port=15873 2>&1 >> rangeserver.rs2.output&

sleep 10

echo "About to load data ... " + `date`

#
# Load Test
#

$HT_HOME/bin/ht ht_load_generator update \
    --Hypertable.Mutator.FlushDelay=20 \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=50000 \
    --rowkey.component.0.order=random \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.min=0 \
    --rowkey.component.0.max=1000000 \
    --row-seed=2 \
    --Field.value.size=5000 \
    --max-bytes=100000000 2>&1 > load.output&

LOAD_PID=${!}

for ((i=0; i<30; i++)) ; do
  HQL_COMMAND=`$SCRIPT_DIR/generate_range_move.py 4`
  echo "Issuing HQL: $HQL_COMMAND"
  echo $HQL_COMMAND | $HT_HOME/bin/ht shell --batch
done

wait $LOAD_PID

echo "use '/'; select * from LoadTest KEYS_ONLY;" | $HT_HOME/bin/ht shell --batch > dump.output

if [ ! -e dump.golden ] ; then
  cp $SCRIPT_DIR/dump.golden.gz .
  gzip -d dump.golden.gz
fi

diff dump.output dump.golden
if [ $? != 0 ] ; then
  sleep 86400
  save_failure_state
  kill_range_servers
  $HT_HOME/bin/clean-database.sh
  exit 1
fi

$HT_HOME/bin/ht ht_load_generator query \
    --rowkey.component.0.order=random \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.min=0 \
    --rowkey.component.0.max=1000000 \
    --row-seed=3 \
    --Field.value.size=1000 \
    --max-keys=50000 2>&1 > query.output&

QUERY_PID=${!}

for ((i=0; i<15; i++)) ; do
  HQL_COMMAND=`$SCRIPT_DIR/generate_range_move.py 4`
  echo "Issuing HQL: $HQL_COMMAND"
  echo $HQL_COMMAND | $HT_HOME/bin/ht shell --batch
done

wait $QUERY_PID

fgrep ERROR query.output > errors.txt
if [ -s errors.txt ] ; then
  save_failure_state
  kill_range_servers
  $HT_HOME/bin/clean-database.sh
  exit 1
fi

kill_range_servers
let iteration=0
$HT_HOME/bin/ht metalog_dump /hypertable/servers/rs1/log/rsml | fgrep "load_acknowledged=false" > errors.txt
$HT_HOME/bin/ht metalog_dump /hypertable/servers/rs2/log/rsml | fgrep "load_acknowledged=false" >> errors.txt
while [ -s errors.txt ] && [ $iteration -lt 5 ]; do
  sleep 10
  $HT_HOME/bin/ht metalog_dump /hypertable/servers/rs1/log/rsml | fgrep "load_acknowledged=false" > errors.txt
  $HT_HOME/bin/ht metalog_dump /hypertable/servers/rs2/log/rsml | fgrep "load_acknowledged=false" >> errors.txt
  let iteration=iteration+1
done

#ls core* >> errors.txt

if [ -s errors.txt ] ; then
  save_failure_state
  kill_range_servers
  $HT_HOME/bin/clean-database.sh
  exit 1
fi

$HT_HOME/bin/clean-database.sh

exit 0


