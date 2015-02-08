#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/ht-start-test-servers.sh --clear --no-thriftbroker

cat $SCRIPT_DIR/hql-create-alter-table.hql | $HT_HOME/bin/ht shell --test-mode >& hql-create-alter-table.output

diff $SCRIPT_DIR/hql-create-alter-table.golden hql-create-alter-table.output
if [ $? -ne 0 ]; then
    $HT_HOME/bin/ht-stop-servers.sh
    echo "error: diff returned non-zero, exiting..."
    exit 1
fi

#
# CREATE TABLE ... WITH functionality
#
echo "USE create_test; DROP TABLE IF EXISTS foo; DROP TABLE IF EXISTS bar;" | $HT_HOME/bin/ht shell --test-mode
echo "USE create_test; CREATE TABLE foo (a, b, c);" | $HT_HOME/bin/ht shell --test-mode
echo "USE create_test; DESCRIBE TABLE WITH IDS foo;" | $HT_HOME/bin/ht shell --batch > foo-schema.txt
echo "USE create_test; CREATE TABLE bar WITH 'foo-schema.txt';" | $HT_HOME/bin/ht shell --test-mode
echo "USE create_test; DESCRIBE TABLE WITH IDS bar;" | $HT_HOME/bin/ht shell --batch > bar-schema.txt
diff foo-schema.txt bar-schema.txt
if [ $? -ne 0 ]; then
    echo "error: 'diff foo-schema.txt bar-schema.txt' returned non-zero, exiting..."
    exit 1
fi

#
# ALTER TABLE ... WITH functionality
#
echo "USE create_test; ALTER TABLE foo DROP (a) ADD (d);" | $HT_HOME/bin/ht shell --test-mode
echo "USE create_test; DESCRIBE TABLE WITH IDS foo;" | $HT_HOME/bin/ht shell --batch > foo-schema.txt
echo "USE create_test; ALTER TABLE bar WITH 'foo-schema.txt';" | $HT_HOME/bin/ht shell --test-mode
echo "USE create_test; DESCRIBE TABLE WITH IDS bar;" | $HT_HOME/bin/ht shell --batch > bar-schema.txt
diff foo-schema.txt bar-schema.txt
if [ $? -ne 0 ]; then
    echo "error: 'diff foo-schema.txt bar-schema.txt' returned non-zero, exiting..."
    exit 1
fi

$HT_HOME/bin/ht-stop-servers.sh
exit 0

