#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/start-test-servers.sh --clear

echo "[test-1]" > thrift-table-refresh.output

# create table
echo "use '/'; create table foo (a, b, c);" | $HT_HOME/bin/ht shell --no-prompt

# insert data
echo "use '/'; INSERT INTO foo VALUES (\"row01\", \"a\", \"hello, world\");" | $HT_HOME/bin/ht shell --no-prompt

# drop and recreate table
echo "use '/'; drop table foo; create table foo (a, b, c);" | $HT_HOME/bin/ht shell --no-prompt

# insert more data
echo "use '/'; INSERT INTO foo VALUES (\"row02\", \"a\", \"hello, world\");" | $HT_HOME/bin/ht shell --no-prompt

# select 
echo "use '/'; SELECT * FROM foo;" | $HT_HOME/bin/ht shell --batch >> thrift-table-refresh.output

echo "[test-2]" >> thrift-table-refresh.output

# insert more data via thrift
$SCRIPT_DIR/thrift_insert.py foo row03 b

# select 
echo "use '/'; SELECT * FROM foo;" | $HT_HOME/bin/ht shell --batch >> thrift-table-refresh.output

echo "[test-3]" >> thrift-table-refresh.output

# insert more data
echo "use '/'; INSERT INTO foo VALUES (\"row04\", \"c\", \"hello, world\");" | $HT_HOME/bin/ht shell --no-prompt

# query via thrift
$SCRIPT_DIR/thrift_query.py foo >>  thrift-table-refresh.output

echo "[test-4]" >> thrift-table-refresh.output

# alter table
echo "use '/'; ALTER TABLE foo ADD ( d );" | $HT_HOME/bin/ht shell --no-prompt

# insert more data
echo "use '/'; INSERT INTO foo VALUES (\"row05\", \"d\", \"hello, world\");" | $HT_HOME/bin/ht shell --no-prompt

# query via thrift
$SCRIPT_DIR/thrift_query.py foo >>  thrift-table-refresh.output

echo "[test-5]" >> thrift-table-refresh.output

# alter table
echo "use '/'; ALTER TABLE foo ADD ( e );" | $HT_HOME/bin/ht shell --no-prompt

# insert more data
echo "use '/'; INSERT INTO foo VALUES (\"row06\", \"e\", \"hello, world\");" | $HT_HOME/bin/ht shell --no-prompt

# insert more data via thrift
$SCRIPT_DIR/thrift_insert.py foo row07 d

# select 
echo "use '/'; SELECT * FROM foo;" | $HT_HOME/bin/ht shell --batch >> thrift-table-refresh.output

echo "[test-6]" >> thrift-table-refresh.output

# drop table and recreate
echo "use '/'; DROP TABLE foo; create table foo (a, b, c);" | $HT_HOME/bin/ht shell --no-prompt

# insert data
echo "use '/'; INSERT INTO foo VALUES (\"row08\", \"a\", \"hello, world\");" | $HT_HOME/bin/ht shell --no-prompt

# insert more data via thrift
$SCRIPT_DIR/thrift_insert.py foo row09 c

# query via thrift
$SCRIPT_DIR/thrift_query.py foo >>  thrift-table-refresh.output

echo "[test-7]" >> thrift-table-refresh.output

# select 
echo "use '/'; SELECT * FROM foo;" | $HT_HOME/bin/ht shell --batch >> thrift-table-refresh.output

echo "[test-8]" >> thrift-table-refresh.output

# alter table via thrift
$SCRIPT_DIR/thrift_hql.py "ALTER TABLE foo ADD ( f )" >>  thrift-table-refresh.output

# insert data
echo "use '/'; INSERT INTO foo VALUES (\"row10\", \"f\", \"hello, world\");" | $HT_HOME/bin/ht shell --no-prompt

# select 
echo "use '/'; SELECT * FROM foo;" | $HT_HOME/bin/ht shell --batch >> thrift-table-refresh.output

echo "[test-9]" >> thrift-table-refresh.output

# alter table via thrift
$SCRIPT_DIR/thrift_hql.py "ALTER TABLE foo ADD ( g )" >>  thrift-table-refresh.output

# select 
echo "use '/'; SELECT * FROM foo;" | $HT_HOME/bin/ht shell --batch >> thrift-table-refresh.output

echo "[test-10]" >> thrift-table-refresh.output

# drop table via thrift
$SCRIPT_DIR/thrift_hql.py "DROP TABLE foo" >>  thrift-table-refresh.output

# select 
echo "use '/'; SELECT * FROM foo;" | $HT_HOME/bin/ht shell --batch 2>&1 | fgrep -v "Table.cc" >> thrift-table-refresh.output

# query via thrift
$SCRIPT_DIR/thrift_query.py foo  2>&1 | fgrep "Hypertable::Exception" | awk 'BEGIN { FS=":" } { print $NF; }' >> thrift-table-refresh.output

# check diff
diff thrift-table-refresh.output $SCRIPT_DIR/thrift-table-refresh.golden
