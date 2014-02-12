#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker

echo "CREATE NAMESPACE MATCH_VERSION; quit;" | $HT_HOME/bin/ht shell --batch

echo "USE MATCH_VERSION; CREATE TABLE VERSION('M' MAX_VERSIONS 10); quit;" | $HT_HOME/bin/ht shell --batch
	
echo "USE MATCH_VERSION; INSERT INTO VERSION VALUES('rowname', 'M', 'VALUE');  quit;" | $HT_HOME/bin/ht shell --batch
echo "USE MATCH_VERSION; INSERT INTO VERSION VALUES('rowname', 'M', 'VALUE');  quit;" | $HT_HOME/bin/ht shell --batch
sleep 2
LAST_TS=`date +"%F %T"`
echo "USE MATCH_VERSION; INSERT INTO VERSION VALUES('$LAST_TS', 'rowname', 'M', 'VALUE');  quit;" | $HT_HOME/bin/ht shell --batch

echo "[first dump]"
echo "USE MATCH_VERSION; SELECT M FROM VERSION DISPLAY_TIMESTAMPS;" | $HT_HOME/bin/ht shell --batch | tee "dump1.tsv"

# Delete last insert
echo "USE MATCH_VERSION; DELETE M FROM VERSION WHERE ROW='rowname' VERSION '$LAST_TS';"
echo "USE MATCH_VERSION; DELETE M FROM VERSION WHERE ROW='rowname' VERSION '$LAST_TS';" | $HT_HOME/bin/ht shell --batch

echo "[second dump]"
echo "USE MATCH_VERSION; SELECT M FROM VERSION DISPLAY_TIMESTAMPS;" | $HT_HOME/bin/ht shell --batch | tee "dump2.tsv"

COUNT=`wc -l dump2.tsv | cut -f1 -d' '`

if [ $COUNT -ne 2 ]; then
  echo "dump2.tsv does not contain exactly two lines!"
  exit 1
fi

exit 0
