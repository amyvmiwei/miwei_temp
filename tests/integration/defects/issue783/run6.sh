#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #783 - LOAD 2"
echo "======================="

function wait_for_file()
{
  file=$1
  echo "wait_for_file $file"
  while [ ! -f $file ]
  do
    sleep 2
  done
  ls -l $file
}

function grep_or_exit_if_found()
{
  needle=$1
  haystack=$2
  grep "$needle" $haystack
  if [ $? -eq 0 ];
  then
    kill `cat rs1.pid`
    kill `cat rs2.pid`
    kill `cat rs3.pid`
    echo "found '$1' in $2 but it shouldn't be there"
    exit -1
  fi
}

function grep_or_exit_if_not_found()
{
  needle=$1
  haystack=$2
  grep "$needle" $haystack
  if [ $? -ne 0 ];
  then
    kill `cat rs1.pid`
    kill `cat rs2.pid`
    kill `cat rs3.pid`
    echo "did not find '$1' in $2 but it should be there"
    exit -1
  fi
}

# delete old monitoring data
\rm -rf $HT_HOME/run/monitoring
\rm -rf $HT_HOME/log/*

# start the cluster with 2 RangeServers and load them with data
$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker --no-rangeserver \
     --Hypertable.Monitoring.Interval=3000
sleep 5
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=rs1.pid \
     --Hypertable.RangeServer.ProxyName=rs1 \
     --Hypertable.RangeServer.Port=38060 \
     --Hypertable.LoadMetrics.Interval=10 \
     '--induce-failure=report-metrics-immediately:signal:0' \
     --Hypertable.RangeServer.Maintenance.Interval 100 \
     --Hypertable.RangeServer.Range.SplitSize=400K 2>1 > rangeserver.rs1.output&
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=rs2.pid \
     --Hypertable.RangeServer.ProxyName=rs2 \
     --Hypertable.RangeServer.Port=38061 \
     --Hypertable.LoadMetrics.Interval=10 \
     '--induce-failure=fsstat-disk-full:signal:0;report-metrics-immediately:signal:0' \
     --Hypertable.RangeServer.Maintenance.Interval 100 \
     --Hypertable.RangeServer.Range.SplitSize=400K 2>1 > rangeserver.rs2.output&
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=rs3.pid \
     --Hypertable.RangeServer.ProxyName=rs3 \
     --Hypertable.RangeServer.Port=38062 \
     --Hypertable.LoadMetrics.Interval=10 \
     '--induce-failure=fsstat-disk-full:signal:0;report-metrics-immediately:signal:0' \
     --Hypertable.RangeServer.Maintenance.Interval 100 \
     --Hypertable.RangeServer.Range.SplitSize=400K 2>1 > rangeserver.rs3.output&
sleep 3
$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql
$HT_HOME/bin/ht ht_load_generator update --spec-file=${SCRIPT_DIR}/data.spec \
        --table=BalanceTest --max-keys=100000 2>&1

# wait till the rrd data from both RangeServers is available
wait_for_file $HT_HOME/run/monitoring/rangeservers/rs1_stats_v0.rrd
wait_for_file $HT_HOME/run/monitoring/rangeservers/rs2_stats_v0.rrd
wait_for_file $HT_HOME/run/monitoring/rangeservers/rs3_stats_v0.rrd

# compact ranges and wait a bit till RS_METRICS is populated
${HT_HOME}/bin/ht rsclient --exec "COMPACT RANGES USER; WAIT FOR MAINTENANCE;"

sleep 15

# dump all keys
${HT_HOME}/bin/ht shell --no-prompt --Hypertable.Request.Timeout=30000 --exec "USE '/'; SELECT * FROM BalanceTest KEYS_ONLY INTO FILE 'dump.pre';"

# balance ranges based on load average
${HT_HOME}/bin/ht shell --no-prompt --exec "BALANCE ALGORITHM='LOAD';"

sleep 10

# make sure that no range was moved to another machine
grep_or_exit_if_found "dest_location=rs2" $HT_HOME/log/Hypertable.Master.log
grep_or_exit_if_found "dest_location=rs3" $HT_HOME/log/Hypertable.Master.log
grep_or_exit_if_not_found "RangeServer rs2: disk use 100% exceeds threshold" \
    $HT_HOME/log/Hypertable.Master.log
grep_or_exit_if_not_found "RangeServer rs2: disk use 100% exceeds threshold" \
    $HT_HOME/log/Hypertable.Master.log

# once more dump all keys
${HT_HOME}/bin/ht shell --no-prompt --Hypertable.Request.Timeout=30000 --exec "USE '/'; SELECT * FROM BalanceTest KEYS_ONLY INTO FILE 'dump.post';"

# clean up before leaving
kill `cat rs1.pid`
kill `cat rs2.pid`
kill `cat rs3.pid`

# make sure that before/after we have the same keys
diff dump.pre dump.post
if [ $? -ne 0 ];
then
  echo "keys differ; exiting"
  exit -1
fi


echo "SUCCESS"
