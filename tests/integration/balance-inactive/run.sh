#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
NUM_POLLS=${NUM_POLLS:-"10"}
WRITE_SIZE=${WRITE_SIZE:-"40000000"}
RS1_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs1.pid
RS2_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs2.pid
MASTER_PIDFILE=$HT_HOME/run/Hypertable.Master.pid

kill_servers() {
  echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:15871
  echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:15870
  kill -9 `cat $RS1_PIDFILE`
  kill -9 `cat $RS2_PIDFILE`
  kill -9 `cat $MASTER_PIDFILE`
  \rm -f $RS1_PIDFILE $RS2_PIDFILE
  $HT_HOME/bin/ht clean-database.sh
}

$HT_HOME/bin/start-test-servers.sh --clear --no-rangeserver --no-master --no-thriftbroker

$HT_HOME/bin/ht Hypertable.Master --verbose --pidfile=$MASTER_PIDFILE \
   2>&1 > master.output &

sleep 3

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

# create sys/RS_METRICS and fill it with data for a few rangeservers
$HT_HOME/bin/ht shell --namespace /sys --no-prompt --exec "LOAD DATA INFILE '$SCRIPT_DIR/rs_metrics.txt' INTO TABLE RS_METRICS;"

sleep 30

# run load balancer 
$HT_HOME/bin/ht shell --no-prompt --exec 'BALANCE ALGORITHM="LOAD";'

# now wait
sleep 30

# and make sure that rs3 and rs4 did not receive any ranges because they
# do not exist
$HT_HOME/bin/ht shell --namespace /sys --no-prompt --exec 'SELECT Location FROM
METADATA REVS=1;' > locations
RET=0
grep "RangeServer rs3 not connected, skipping" master.output
if [ $? -ne "0" ] ; then
  echo "rs3 got locations assigned"
  RET=1
fi
grep "RangeServer rs4 not connected, skipping" master.output
if [ $? -ne "0" ] ; then
  echo "rs4 got locations assigned"
  RET=1
fi
grep rs3 locations
if [ $? -eq "0" ] ; then
  echo "rs3 got locations assigned"
  RET=1
fi
grep rs4 locations
if [ $? -eq "0" ] ; then
  echo "rs4 got locations assigned"
  RET=1
fi

sleep 1

kill_servers

exit $RET


