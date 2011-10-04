#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME
SCRIPT_DIR=`dirname $0`
NUM_POLLS=${NUM_POLLS:-"10"}
MY_IP=`$HT_HOME/bin/system_info --my-ip`
WRITE_TOTAL=${WRITE_TOTAL:-"30000000"}
WRITE_SIZE=${WRITE_SIZE:-"500000"}

. $HT_HOME/bin/ht-env.sh

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
   --Hypertable.Master.Gc.Interval=5000 \
   --Hypertable.RangeServer.CommitLog.RollLimit 1M \
   --Hypertable.RangeServer.CommitLog.Compressor none \
   --Hypertable.RangeServer.Maintenance.Interval 100 \
   --Hypertable.RangeServer.Range.SplitSize=500K

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht_load_generator update \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=10K \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.min=${i}10000 \
    --rowkey.component.0.max=${i}90000 \
    --Field.value.size=10 \
    --max-bytes=5000000

find $HT_HOME/fs/local/hypertable/tables/ -name 'cs*' -print

sleep 1

while [ $NUM_POLLS -gt 0 ]; do

  find $HT_HOME/fs/local/hypertable/tables/ -regex '.*/cs[0-9]*' -print | awk 'BEGIN { FS="/tables/" } { print $NF }' | sed 's/^\/*//g' | sort > FILESYSTEM.files

  echo "use '/sys'; select Files from METADATA revs=1;" | $HT_HOME/bin/ht shell --batch | cut -f3 | awk 'BEGIN { FS=";" } { for (i = 1; i <= NF; i++) if ($i != "\\n") print $i }' | sort | uniq > METADATA.files

  diff FILESYSTEM.files METADATA.files

  if [ $? == 0 ] ; then
    exit 0
  fi

  sleep 5

  NUM_POLLS=$((NUM_POLLS - 1))
done

echo "ERROR: cellstores not garbaged collected:"
diff FILESYSTEM.files METADATA.files
exit 1
