#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

. $HT_HOME/bin/ht-env.sh

$HT_HOME/bin/ht start-test-servers --clear --no-thriftbroker \
    --Hypertable.Master.Split.SoftLimitEnabled=false

echo "use '/'; drop table if exists LoadTest; CREATE TABLE LoadTest (Column1);" | $HT_HOME/bin/ht shell --batch

# Try load generator with --no-log switch
$HT_HOME/bin/ht load_generator update --spec-file=$SCRIPT_DIR/data.spec --max-bytes=5M --no-log

# Try LOAD DATA INFILE with NO_LOG option
echo "use '/'; load data infile NO_LOG '$SCRIPT_DIR/data.tsv' into table LoadTest;" | $HT_HOME/bin/ht shell --batch

$HT_HOME/bin/ht stop-servers

LOG_SIZE=`ls -l $HT_HOME/fs/local/hypertable/servers/rs1/log/user/0 | tr -s " " | cut -f5 -d' '`
if [ $LOG_SIZE -lt 8  ]; then
  echo "error: user commit log is not empty"
  exit 1
fi

exit 0
