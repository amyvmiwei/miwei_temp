#!/bin/bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/start-test-servers.sh --clean

#
# Generate prune_test.tsv
#
echo "#row	timestamp	c" > prune_test.tsv
let sse=`date +%s`
let sse=$(($sse-(8*86400)+3600))
echo $sse
for ((i=0; i<8; i++)); do
  echo "apple	`date -d @$sse +"%Y-%m-%d %H:%M:%S"`	$i" >> prune_test.tsv
  let sse=sse+86400
done

echo "CREATE NAMESPACE test;" | $HT_HOME/bin/ht shell --batch

echo "USE 'test';
DROP TABLE IF EXISTS prune_tsv_test;
CREATE TABLE prune_tsv_test ( c );
LOAD DATA INFILE TIMESTAMP_COLUMN=timestamp ROW_KEY_COLUMN=row+timestamp \"prune_test.tsv\" INTO TABLE prune_tsv_test;
DUMP TABLE prune_tsv_test INTO FILE \"prune_tsv_test.output\";" | $HT_HOME/bin/ht shell --test-mode

./prune_tsv --newer --field 1 4d < prune_tsv_test.output > regenerated.output
if [ $? -ne 0 ]; then
    echo "prune_tsv failure"
    exit 1
fi

./prune_tsv --field 1 4d < prune_tsv_test.output | grep -v "^#" >> regenerated.output
if [ $? -ne 0 ]; then
    echo "prune_tsv failure"
    exit 1
fi

diff prune_tsv_test.output regenerated.output
if [ $? -ne 0 ]; then
    echo "diff failure"
    exit 1
fi

./prune_tsv --newer 4d < prune_tsv_test.output > regenerated.output
if [ $? -ne 0 ]; then
    echo "prune_tsv failure"
    exit 1
fi

./prune_tsv 4d < prune_tsv_test.output | grep -v "^#" >> regenerated.output
if [ $? -ne 0 ]; then
    echo "prune_tsv failure"
    exit 1
fi

diff prune_tsv_test.output regenerated.output
if [ $? -ne 0 ]; then
    echo "diff failure"
    exit 1
fi

exit 0
