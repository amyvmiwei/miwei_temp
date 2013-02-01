DIGEST="openssl dgst -md5"
MASTER_LOG=$HT_HOME/log/Hypertable.Master.log
MASTER_PIDFILE=$HT_HOME/run/Hypertable.Master.pid
ROW_SEED=1

start_master() {
  set_start_vars Hypertable.Master
  check_pidfile $pidfile && return 0

  check_server --config=${SCRIPT_DIR}/test.cfg master
  if [ $? != 0 ] ; then
      $HT_HOME/bin/ht Hypertable.Master --verbose --pidfile=$MASTER_PIDFILE \
          --config=${SCRIPT_DIR}/test.cfg 2>&1 > $MASTER_LOG&
    wait_for_server_up master "$pidname" --config=${SCRIPT_DIR}/test.cfg
  else
    echo "WARNING: $pidname already running."
  fi
}

wait_for_server_connect() {
    grep RS_METRICS $HT_HOME/log/Hypertable.Master.log
    while [ $? -ne 0 ] ; do
        sleep 3
        grep RS_METRICS $HT_HOME/log/Hypertable.Master.log
    done
}

wait_for_recovery() {
  local id=$1
  local n=0
  local s="Leaving RecoverServer $id state=COMPLETE"
  egrep "$s" $HT_HOME/log/Hypertable.Master.log
  while [ $? -ne "0" ]
  do
    (( n += 1 ))
    if [ "$n" -gt "300" ]; then
      echo "wait_for_recovery: time exceeded"
      exit 1
    fi
    sleep 2
    egrep "$s" $HT_HOME/log/Hypertable.Master.log
  done
}

gen_test_data() {
    if [ ! -s golden_dump.$MAX_KEYS.md5 ] ; then
        $HT_HOME/bin/ht load_generator --spec-file=$SCRIPT_DIR/data.spec \
            --max-keys=$MAX_KEYS --row-seed=$ROW_SEED --table=LoadTest \
            --stdout update | cut -f1 | tail -n +2 | sort -u > golden_dump.$MAX_KEYS.txt
        $DIGEST < golden_dump.$MAX_KEYS.txt > golden_dump.$MAX_KEYS.md5
        #\rm -f golden_dump.$MAX_KEYS.txt
    fi
}

dump_keys() {
    $HT_HOME/bin/ht shell -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
        | grep -v "Waiting for connection to Hyperspace" > $1.txt
    if [ $? != 0 ] ; then
        echo "Problem dumping table 'LoadTest', exiting ..."
        exit 1
    fi
    $DIGEST < $1.txt > $1.md5
    diff golden_dump.$MAX_KEYS.md5 $1.md5
    if [ $? != 0 ] ; then
        echo "Test $TEST FAILED."
        $HT_HOME/bin/ht shell -l error --batch < $SCRIPT_DIR/dump-test-table.hql \
            | grep -v "Waiting for connection to Hyperspace" > $1.again.txt
        exec 1>&-
        sleep 86400
    fi
}


stop_rs() {
    local port
    let port=38059+$1
    echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:$port
    kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs$1.pid`
    \rm -f $HT_HOME/run/Hypertable.RangeServer.rs$1.pid
}

kill_rs() {
    for num in "$@"; do
        kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs${num}.pid`
        \rm -f $HT_HOME/run/Hypertable.RangeServer.rs${num}.pid
    done
}

kill_all_rs() {
    kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs*.pid`
    \rm -f $HT_HOME/run/Hypertable.RangeServer.rs*.pid
    kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.pid`
    \rm -f $HT_HOME/run/Hypertable.RangeServer.pid
}

save_failure_state() {
    local label=$1
    shift
    mkdir -p failed-run-$label
    \rm -f $HT_HOME/run/op.output
    touch $HT_HOME/run/debug-op
    ps auxww | fgrep -i hyper | fgrep -v java > failed-run-$label/ps-output.txt
    cp $HT_HOME/log/* failed-run-$label
    pstack `cat $HT_HOME/run/Hypertable.Master.pid` > failed-run-$label/master-stack.txt
    sleep 60
    cp $HT_HOME/run/op.output failed-run-$label
    cp rangeserver.* failed-run-$label
    cp rangeserver.*.output failed-run-$label
    cp master.* failed-run-$label
}
