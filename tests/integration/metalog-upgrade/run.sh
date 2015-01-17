#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

function finish {
    $HT_HOME/bin/stop-servers.sh
}
trap finish EXIT

. $HT_HOME/bin/ht-env.sh

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker --no-rangeserver --no-master

#
# MML
#
rm -f mml.output
echo "mkdirs /hypertable/servers/tmp/mml" | $HT_HOME/bin/ht fsclient --batch
for d in $SCRIPT_DIR/mml/* ; do
  echo "MML Version `basename $d`" >> mml.output
  for f in $d/mml/* ; do
    echo "--- " `basename $f` " ---" >> mml.output
    echo "copyFromLocal $f /hypertable/servers/tmp/mml/0" | $HT_HOME/bin/ht fsclient --batch
    $HT_HOME/bin/ht metalog_dump --all /hypertable/servers/tmp/mml >> mml.output
    if [ $? -ne 0 ]; then
      echo "MML metalog_dump failure"
      exit 1
    fi
  done
done

diff mml.output $SCRIPT_DIR/mml.golden
if [ $? -ne 0 ]; then
  echo "MML upgrade test failure"
  exit 1
fi

#
# RSML
#
rm -f rsml.output
echo "mkdirs /hypertable/servers/tmp/rsml" | $HT_HOME/bin/ht fsclient --batch
for d in $SCRIPT_DIR/rsml/* ; do
  echo "RSML Version `basename $d`" >> rsml.output
  for f in $d/rsml/* ; do
    echo "--- " `basename $f` " ---" >> rsml.output
    echo "copyFromLocal $f /hypertable/servers/tmp/rsml/0" | $HT_HOME/bin/ht fsclient --batch
    $HT_HOME/bin/ht metalog_dump --all /hypertable/servers/tmp/rsml >> rsml.output
    if [ $? -ne 0 ]; then
      echo "RSML metalog_dump failure"
      exit 1
    fi
  done
done

diff rsml.output $SCRIPT_DIR/rsml.golden
if [ $? -ne 0 ]; then
  echo "RSML upgrade test failure"
  exit 1
fi

