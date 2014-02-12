#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
#DATA_SEED=42 # for repeating certain runs
MAX_KEYS=${MAX_KEYS:-"500"} 
RS1_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs1.pid
RS2_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs2.pid
RUN_DIR=`pwd`
DUMPFILE="$RUN_DIR/dump_keys.out"
RS1_RANGES="$RUN_DIR/rs1_ranges.out"
RS2_RANGES="$RUN_DIR/rs2_ranges.out"

. $HT_HOME/bin/ht-env.sh

stop_rs2() {
  echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:15871
  sleep 1
  kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs?.pid`
  \rm -f $HT_HOME/run/Hypertable.RangeServer.rs?.pid
}

stop_rs1() {
  echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:15870
  sleep 1
  kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs1.pid`
  \rm -f $HT_HOME/run/Hypertable.RangeServer.rs1.pid
}

compact_user() {
  echo "compact ranges user; quit;" | $HT_HOME/bin/ht rsclient localhost:15870
  echo "compact ranges user; quit;" | $HT_HOME/bin/ht rsclient localhost:15871
}

dump_rs_ranges() {
  echo "dump nokeys '$RS1_RANGES';" | $HT_HOME/bin/ht rsclient localhost:15870
  echo "dump nokeys '$RS2_RANGES';" | $HT_HOME/bin/ht rsclient localhost:15871
}



# stop and start servers
rm *.out
touch $DUMPFILE
$HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker \
    --clear --config=${SCRIPT_DIR}/test.cfg
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs1 \
   --Hypertable.RangeServer.Port=15870 --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs1.out&
sleep 10
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs2 \
   --Hypertable.RangeServer.Port=15871 --config=${SCRIPT_DIR}/test.cfg 2>&1 > rangeserver.rs2.out&

#create table
$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

#write data 
$HT_HOME/bin/ht ht_load_generator update \
    --Hypertable.Mutator.FlushDelay=50 \
    --rowkey.component.0.order=ascending\
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.min=0 \
    --rowkey.component.0.max=1000000 \
    --row-seed=1 \
    --Field.value.size=20000 \
    --max-keys=$MAX_KEYS

#let splits happen and then compact user ranges
sleep 15
compact_user
sleep 15

#dump METADATA location
${HT_HOME}/bin/ht shell --no-prompt --exec "use sys; select Location from METADATA MAX_VERSIONS=1 into file '${RUN_DIR}/metadata_pre.out';"

#offload ranges from rs1
${HT_HOME}/bin/ht shell --no-prompt --exec "BALANCE ALGORITHM='OFFLOAD rs1';"

#wait for balance to complete 
sleep 20 

dump_rs_ranges
#kill rs1
stop_rs1

#verify keys
${HT_HOME}/bin/ht shell --no-prompt --Hypertable.Request.Timeout=30000 --exec "use '/'; select * from LoadTest keys_only into file '${DUMPFILE}';"

#dump METADATA location
${HT_HOME}/bin/ht shell --no-prompt --Hypertable.Request.Timeout=30000 --exec "use sys; select Location from METADATA MAX_VERSIONS=1 into file '${RUN_DIR}/metadata_post.out';"

#shut everything down
cp ${HT_HOME}/log/Hypertable.Master.log ${RUN_DIR}/master.out
stop_rs2
$HT_HOME/bin/clean-database.sh

#make sure we got all the data back and all ranges are on rs2 
NUM_KEYS=`cat ${DUMPFILE} | grep -v '#'|wc -l`;
if (( $NUM_KEYS != $MAX_KEYS )) 
then
  echo "Test failed, expected $MAX_KEYS keys, got  $NUM_KEYS"
  exit 1
fi

TOTAL_RANGES=`grep Location metadata_post.out|wc -l`
RS1_RANGES=`grep "rs1" metadata_post.out|wc -l`
RS2_RANGES=`grep "rs2" metadata_post.out|wc -l`
echo "Total ranges=${TOTAL_RANGES}, rs1 ranges=${RS1_RANGES}, rs2 ranges=${RS2_RANGES}"
#all ranges should be on rs2 
if (( $RS1_RANGES != 0 || $TOTAL_RANGES != $RS2_RANGES))
then
  echo "Test failed"
  exit 1
else 
  echo "Test passed"
fi
exit 0
