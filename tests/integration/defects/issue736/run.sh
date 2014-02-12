#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
MAX_KEYS=${MAX_KEYS:-"200000"}
RS1_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs1.pid
RS2_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs2.pid
RUN_DIR=`pwd`

. $HT_HOME/bin/ht-env.sh

\rm -f output

wait_for_recovery() {
  grep "Leaving RecoverServer rs1 state=COMPLETE" \
      $HT_HOME/log/Hypertable.Master.log
  while [ $? -ne "0" ]
  do
    sleep 2
    grep "Leaving RecoverServer rs1 state=COMPLETE" \
        $HT_HOME/log/Hypertable.Master.log
  done
}

stop_rs1() {
  kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs1.pid`
}

stop_rs2() {
  kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs2.pid`
}

\rm -rf ${HYPERTABLE_HOME}/log/*

# install our sample hook
cp ${HYPERTABLE_HOME}/conf/notification-hook.sh notification-hook.bak
cp notification-hook.sh ${HYPERTABLE_HOME}/conf/notification-hook.sh

# stop and start servers
$HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker \
    --clear --config=${SCRIPT_DIR}/test.cfg
# start both rangeservers
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs1 \
   --Hypertable.RangeServer.Port=15870 --config=${SCRIPT_DIR}/test.cfg 2>1 > rangeserver.rs1.output&
sleep 10
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs2 \
   --Hypertable.RangeServer.Port=15871 --config=${SCRIPT_DIR}/test.cfg 2>1 > rangeserver.rs2.output&

# create table
$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

# write data 
$HT_HOME/bin/ht ht_load_generator update \
    --Hypertable.Mutator.FlushDelay=50 \
    --rowkey.component.0.order=random \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%020lld" \
    --rowkey.component.0.min=0 \
    --rowkey.component.0.max=10000000000 \
    --row-seed=1 \
    --Field.value.size=200 \
    --max-keys=$MAX_KEYS

sleep 5

# kill rs1
stop_rs1

# wait for recovery to complete 
wait_for_recovery

# stop servers
$HT_HOME/bin/stop-servers.sh
stop_rs2

# check if the hook was executed
sed "s/HOSTNAME/`hostname`/" ${SCRIPT_DIR}/issue736.golden > golden
diff output golden

if [ "$?" -ne "0" ]
then
  echo "Test failed, golden file differs"
  # restore the old hook
  cp notification-hook.bak ${HYPERTABLE_HOME}/conf/notification-hook.sh
  exit 1
fi

# restore the old hook
cp notification-hook.bak ${HYPERTABLE_HOME}/conf/notification-hook.sh

echo "Test passed"
exit 0
