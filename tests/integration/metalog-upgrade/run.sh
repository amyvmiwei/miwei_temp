#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

function finish {
    $HT_HOME/bin/stop-servers.sh
}
trap finish EXIT

. $HT_HOME/bin/ht-env.sh

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker --no-rangeserver --no-master

rm -f mml.output

echo "mkdirs /hypertable/servers/tmp/mml" | $HT_HOME/bin/ht fsclient --batch

for f in $SCRIPT_DIR/mml/* ; do
  echo "--- " `basename $f` " ---" >> mml.output
  echo "copyFromLocal $f /hypertable/servers/tmp/mml/0" | $HT_HOME/bin/ht fsclient --batch
  $HT_HOME/bin/ht metalog_dump /hypertable/servers/tmp/mml >> mml.output
done

cat mml.output | perl -e 'while (<>) { s/timestamp=.*?201\d,/timestamp=0,/g; print; }' > mml.output.filtered

diff mml.output.filtered $SCRIPT_DIR/mml.golden
if [ $? -ne 0 ]; then
  echo "MML upgrade test failure"
  exit 1
fi

