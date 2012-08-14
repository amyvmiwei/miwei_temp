#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"/opt/hypertable/current"}
SCRIPT_DIR=`dirname $0`

echo "======================="
echo "Defect #895"
echo "======================="

BINDIR=`pwd`/../../../../examples/apache_log

#$HT_HOME/bin/ht start-test-servers.sh --clear

echo "creating table"
$HT_HOME/bin/ht shell --namespace / --exec "DROP TABLE IF EXISTS LogDb; CREATE TABLE LogDb ( ClientIpAddress, UserId, Request, ResponseCode, ObjectSize, Referer, UserAgent);"

echo "copying files"
cp $BINDIR/apache_log_load $HT_HOME/bin
cp $BINDIR/apache_log_query $HT_HOME/bin

echo "loading data"
$HT_HOME/bin/apache_log_load $SCRIPT_DIR/../../../../examples/apache_log/access.log.gz
$HT_HOME/bin/ht shell --namespace / --exec "SELECT * FROM LogDb;" \
        --no-prompt --test-mode \
        > test.output

echo "running a test query" >> test.output
$HT_HOME/bin/apache_log_query /favicon.ico >> test.output

echo "cleaning up"
rm $HT_HOME/bin/apache_log_load
rm $HT_HOME/bin/apache_log_query 

diff test.output $SCRIPT_DIR/test.golden
if [ $? != 0 ]
then
  echo "golden files differ"
  exit 1
fi

exit 0
