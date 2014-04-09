CREATE NAMESPACE "/indices";
USE "/indices";
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS test;

# CREATE TABLE - negative tests
CREATE TABLE t (a, INDEX (a));
CREATE TABLE t (a, INDEX b);
CREATE TABLE t (a, INDEX a, INDEX a);
CREATE TABLE "^t" (a, b);

# CREATE TABLE
CREATE TABLE t (a, INDEX a);
SHOW CREATE TABLE t;
DESCRIBE TABLE t;
CREATE TABLE t2 LIKE t;
SHOW CREATE TABLE t2;
DESCRIBE TABLE t2;
DROP TABLE t2;
DROP TABLE t;

CREATE TABLE t (a, b, INDEX a);
SHOW CREATE TABLE t;
DESCRIBE TABLE t;
CREATE TABLE t2 LIKE t;
SHOW CREATE TABLE t2;
DESCRIBE TABLE t2;
DROP TABLE t2;
DROP TABLE t;

CREATE TABLE t (a, b, INDEX a, INDEX b);
SHOW CREATE TABLE t;
DESCRIBE TABLE t;
CREATE TABLE t2 LIKE t;
SHOW CREATE TABLE t2;
DESCRIBE TABLE t2;
DROP TABLE t2;
DROP TABLE t;

# verify that the secondary index table is created
CREATE TABLE t (a, b, c);
SHOW CREATE TABLE "t";
SHOW CREATE TABLE "^t";
DROP TABLE t;

# verify that the secondary index table is created
CREATE TABLE t (a, INDEX a);
SHOW CREATE TABLE "t";
SHOW CREATE TABLE "^t";
DROP TABLE t;
DROP TABLE '^t';

# verify that the secondary index table is created
CREATE TABLE t (a, b, c, INDEX a, INDEX b, INDEX c);
SHOW CREATE TABLE "t";
SHOW CREATE TABLE "^t";
DROP TABLE t;
DROP TABLE '^t';

# INSERT - simple tests
CREATE TABLE t (a, b, c, d, INDEX a, INDEX b);
INSERT INTO t VALUES ("row1", "a", "cell1");
INSERT INTO t VALUES ("row2", "b", "cell2");
INSERT INTO t VALUES ("row3", "c", "cell3");
INSERT INTO t VALUES ("row4", "d", "cell4");
SELECT * FROM "t";
SELECT * FROM "^t";
INSERT INTO t VALUES ("row1", "a", "cell1");
INSERT INTO t VALUES ("row2", "b", "cell2");
INSERT INTO t VALUES ("row3", "c", "cell3");
INSERT INTO t VALUES ("row4", "d", "cell4");
SELECT * FROM "t";
SELECT * FROM "^t";
INSERT INTO t VALUES ("row11", "a", "cell11");
INSERT INTO t VALUES ("row12", "b", "cell12");
INSERT INTO t VALUES ("row13", "c", "cell13");
INSERT INTO t VALUES ("row14", "d", "cell14");
SELECT * FROM "t";
SELECT * FROM "^t";
INSERT INTO t VALUES ("row15", "a", "cell15");
INSERT INTO t VALUES ("row16", "b", "cell16");
INSERT INTO t VALUES ("row17", "c", "cell17");
INSERT INTO t VALUES ("row18", "d", "cell18");
SELECT * FROM "t";
SELECT * FROM "^t";
INSERT INTO t VALUES ("row\t25", "a", "cell\t25");
INSERT INTO t VALUES ("row\t26", "b", "cell\t26");
INSERT INTO t VALUES ("row\t27", "c", "cell\t27");
INSERT INTO t VALUES ("row\t28", "d", "cell\t28");
SELECT * FROM "t";
SELECT * FROM "^t";

DROP TABLE t;
CREATE TABLE t(a, INDEX a);
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa0", "a", "cella0");
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa1", "a", "cella1");
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa2", "a", "cella2");
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa3", "a", "cella3");
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa4", "a", "cella4");
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa10", "a", "mella10");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa5", "a", "cella5");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa6", "a", "cella6");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa7", "a", "cella7");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa8", "a", "cella8");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa9", "a", "cella9");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa11", "a", "mella11");
SELECT a FROM t;
SELECT unknown FROM t;

SELECT a FROM t WHERE a = "cella2";
SELECT a FROM t WHERE a = "cella3";
SELECT a FROM t WHERE a =^ "cella3";
SELECT a FROM t WHERE a =^ "m";
SELECT a FROM t WHERE a =^ "mel";
SELECT a FROM t WHERE "2011-11-10 00:00:00" < TIMESTAMP < "2011-11-11 00:00:00" AND a =^ "m" DISPLAY_TIMESTAMPS;

SELECT a FROM t WHERE (ROW < "rowa3" OR ROW > "rowa7") AND a =^ "m";
SELECT a FROM t WHERE (ROW < "rowa3") AND a =^ "m";
SELECT a FROM t WHERE (ROW < "00") AND a =^ "m";
SELECT a FROM t WHERE ROW REGEXP "rowa" AND a =^ "m";
SELECT a FROM t WHERE ROW REGEXP "^rowa" AND a =^ "m";
SELECT a FROM t WHERE ROW REGEXP "^rowa$" AND a =^ "m";
SELECT a FROM t WHERE ROW REGEXP "rowa$" AND a =^ "m";
SELECT a FROM t WHERE ROW REGEXP "3$" AND a =^ "m";
SELECT a FROM t WHERE ROW REGEXP "\d$" AND a =^ "m";

SELECT a FROM t WHERE (ROW < "rowa3" OR ROW > "rowa7") AND a =^ "c";
SELECT a FROM t WHERE (ROW < "rowa3") AND a =^ "c";
SELECT a FROM t WHERE (ROW < "00") AND a =^ "c";
SELECT a FROM t WHERE ROW REGEXP "rowa" AND a =^ "c";
SELECT a FROM t WHERE ROW REGEXP "^rowa" AND a =^ "c";
SELECT a FROM t WHERE ROW REGEXP "^rowa$" AND a =^ "c";
SELECT a FROM t WHERE ROW REGEXP "rowa$" AND a =^ "c";
SELECT a FROM t WHERE ROW REGEXP "3$" AND a =^ "c";
SELECT a FROM t WHERE ROW REGEXP "\d$" AND a =^ "c";

INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa12", "a", "cella9xx");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa13", "a", "cella9xy");
SELECT a FROM t WHERE a =^ "cella9";
SELECT a FROM t WHERE a =^ "x";
SELECT a FROM t WHERE a =^ "cella9x";

INSERT INTO t VALUES ("rowa3", "a", "cella4");
SELECT a FROM t WHERE a = "cella4";
SELECT a FROM t WHERE a =^ "c" KEYS_ONLY;

INSERT INTO t VALUES ("rowa3", "a", "cella5");
SELECT a FROM t WHERE a =^ "c" REVS = 1;
SELECT a FROM t WHERE a =^ "c" REVS = 2;
SELECT a FROM t WHERE a =^ "c" REVS = 3;
SELECT a FROM t WHERE a =^ "c" CELL_LIMIT = 1;
SELECT a FROM t WHERE a =^ "c" CELL_LIMIT_PER_FAMILY = 1;
SELECT a FROM t WHERE a =^ "c" CELL_LIMIT_PER_FAMILY = 2;
SELECT a FROM t WHERE a =^ "c" CELL_LIMIT_PER_FAMILY = 3;
SELECT a FROM t WHERE a =^ "c" LIMIT = 1;

SELECT a FROM t WHERE a =^ "c" OFFSET 2 LIMIT 3;
SELECT a FROM t WHERE a =^ "c" OFFSET 3 LIMIT 3;
SELECT a FROM t WHERE a =^ "c" OFFSET 2 CELL_LIMIT 3;
SELECT a FROM t WHERE a =^ "c" CELL_OFFSET 2 LIMIT 3;
SELECT a FROM t WHERE a =^ "c" CELL_OFFSET 5 CELL_LIMIT 1;

DELETE * FROM t WHERE ROW = "rowa2";
SELECT a FROM t WHERE a =^ "c";
SELECT a FROM t WHERE a =^ "c" RETURN_DELETES;
SELECT a FROM t WHERE a =^ "c" RETURN_DELETES KEYS_ONLY;

# negative test - index doesn't return any result
SELECT a FROM t WHERE a =^ "x";
# negative test - index returns result but they're skipped in
# IndexResultCallback
SELECT a FROM t WHERE ROW < "a" AND a =^ "c";
# negative test - index returns result but they're outdated
SELECT a FROM t WHERE a =^ "cella2";

# verify that results are sorted by row
INSERT INTO t VALUES ("aaa0", "a", "nyyy");
INSERT INTO t VALUES ("aaa1", "a", "nxxx");
SELECT a FROM t WHERE ROW < "b";
SELECT a FROM t WHERE a =^ "n";

# overwrite a value, make sure that the index still works
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa13", "a", "cella9xz");
SELECT a FROM t WHERE a =^ "cella9";

DROP TABLE IF EXISTS t;
CREATE TABLE t (a, INDEX a, b);
INSERT INTO t VALUES ("row0", "a", "cella0");
INSERT INTO t VALUES ("row1", "a", "cella1");
INSERT INTO t VALUES ("row2", "a", "cella2");
INSERT INTO t VALUES ("row0", "b", "cellb0");
INSERT INTO t VALUES ("row1", "b", "cellb1");
INSERT INTO t VALUES ("row2", "b", "cellb2");

SELECT a FROM t WHERE CELL > "row1","a" AND a =^ "c";
SELECT a FROM t WHERE CELL >= "row1","a" AND a =^ "c";
SELECT a FROM t WHERE CELL < "row2","a" AND a =^ "c";
SELECT a FROM t WHERE CELL <= "row2","a" AND a =^ "c";
SELECT a FROM t WHERE (CELL < "row2","a" or CELL > "row1","a") AND a =^ "c";

DROP TABLE IF EXISTS t;
CREATE TABLE t (a TIME_ORDER DESC MAX_VERSIONS=1, INDEX a);
DESCRIBE TABLE "^t";
INSERT INTO t VALUES ("row0", "a", "cella0");
INSERT INTO t VALUES ("row0", "a", "cella1");
INSERT INTO t VALUES ("row1", "a", "cellb0");
INSERT INTO t VALUES ("row1", "a", "cellb1");
INSERT INTO t VALUES ("row2", "a", "cellc0");
INSERT INTO t VALUES ("row2", "a", "cellc1");
SELECT a FROM t;

DROP TABLE IF EXISTS t;
CREATE TABLE t (a, INDEX a, b, INDEX b);
DESCRIBE TABLE "^t";
INSERT INTO t VALUES ("row00", "a", "cella00");
INSERT INTO t VALUES ("row01", "a", "cella01");
INSERT INTO t VALUES ("row02", "a", "cella02");
INSERT INTO t VALUES ("row03", "a", "cella03");
INSERT INTO t VALUES ("row04", "a", "cella04");
INSERT INTO t VALUES ("row05", "a", "cella05");
INSERT INTO t VALUES ("row06", "a", "cella06");
INSERT INTO t VALUES ("row07", "a", "cella07");
INSERT INTO t VALUES ("row08", "a", "cella08");
INSERT INTO t VALUES ("row09", "a", "cella09");
INSERT INTO t VALUES ("row10", "a", "cella10");
INSERT INTO t VALUES ("row11", "a", "cella11");
INSERT INTO t VALUES ("row12", "a", "cella12");
INSERT INTO t VALUES ("row13", "a", "cella13");
INSERT INTO t VALUES ("row14", "a", "cella14");
INSERT INTO t VALUES ("row15", "a", "cella15");
INSERT INTO t VALUES ("row16", "a", "cella16");
INSERT INTO t VALUES ("row17", "a", "cella17");
INSERT INTO t VALUES ("row18", "a", "cella18");
INSERT INTO t VALUES ("row19", "a", "cella19");
INSERT INTO t VALUES ("row00", "b", "cellb00");
INSERT INTO t VALUES ("row01", "b", "cellb01");
INSERT INTO t VALUES ("row02", "b", "cellb02");
INSERT INTO t VALUES ("row03", "b", "cellb03");
INSERT INTO t VALUES ("row04", "b", "cellb04");
INSERT INTO t VALUES ("row05", "b", "cellb05");
INSERT INTO t VALUES ("row06", "b", "cellb06");
INSERT INTO t VALUES ("row07", "b", "cellb07");
INSERT INTO t VALUES ("row08", "b", "cellb08");
INSERT INTO t VALUES ("row09", "b", "cellb09");
INSERT INTO t VALUES ("row10", "b", "cellb10");
INSERT INTO t VALUES ("row11", "b", "cellb11");
INSERT INTO t VALUES ("row12", "b", "cellb12");
INSERT INTO t VALUES ("row13", "b", "cellb13");
INSERT INTO t VALUES ("row14", "b", "cellb14");
INSERT INTO t VALUES ("row15", "b", "cellb15");
INSERT INTO t VALUES ("row16", "b", "cellb16");
INSERT INTO t VALUES ("row17", "b", "cellb17");
INSERT INTO t VALUES ("row18", "b", "cellb18");
INSERT INTO t VALUES ("row19", "b", "cellb19");

SELECT a FROM t WHERE a =^ "cell" LIMIT 8;
SELECT b FROM t WHERE b =^ "cell" LIMIT 8;
SELECT a FROM t WHERE ROW < "row05" AND a =^ "cell";

SELECT * FROM t WHERE a = "cella2";
SELECT b FROM t WHERE a = "cella2";
SELECT a FROM t WHERE b = "cellb2";

SHOW CREATE TABLE t;
SHOW CREATE TABLE "^t";
ALTER TABLE t RENAME COLUMN FAMILY (a, c);
SHOW CREATE TABLE t;
SHOW CREATE TABLE "^t";
SELECT c FROM t WHERE ROW < "row05" AND c =^ "cell";
SELECT b FROM t WHERE ROW < "row05" AND b =^ "cell";

RENAME TABLE t TO r;
SHOW CREATE TABLE r;
SHOW CREATE TABLE "^r";
SELECT c FROM r WHERE ROW < "row05" AND c =^ "cell";
SELECT b FROM r WHERE ROW < "row05" AND b =^ "cell";
RENAME TABLE r TO t;

# make sure that column qualifiers are working (even if they're not indexed)
INSERT INTO t VALUES ("row20", "b:foo", "cellb20foo");
INSERT INTO t VALUES ("row21", "b:foo", "cellb21foo");
INSERT INTO t VALUES ("row22", "b:foo", "cellb22foo");
INSERT INTO t VALUES ("row20", "b:bar", "cellb20bar");
INSERT INTO t VALUES ("row21", "b:bar", "cellb21bar");
INSERT INTO t VALUES ("row22", "b:bar", "cellb22bar");
SELECT b:foo FROM t WHERE b:* =^ "cell";
SELECT b:bar FROM t WHERE b:* =^ "cell";

# qualifier indices: CREATE TABLE 
DROP TABLE t;
CREATE TABLE t (a, QUALIFIER INDEX a);
SHOW CREATE TABLE t;
DESCRIBE TABLE t;
CREATE TABLE t2 LIKE t;
SHOW CREATE TABLE t2;
DESCRIBE TABLE t2;
DROP TABLE t2;
DROP TABLE t;

CREATE TABLE t (a, b, QUALIFIER INDEX a);
SHOW CREATE TABLE t;
DESCRIBE TABLE t;
CREATE TABLE t2 LIKE t;
SHOW CREATE TABLE t2;
DESCRIBE TABLE t2;
DROP TABLE t2;
DROP TABLE t;

CREATE TABLE t (a, b, QUALIFIER INDEX a, QUALIFIER INDEX b);
SHOW CREATE TABLE t;
DESCRIBE TABLE t;
CREATE TABLE t2 LIKE t;
SHOW CREATE TABLE t2;
DESCRIBE TABLE t2;
DROP TABLE t2;
DROP TABLE t;

CREATE TABLE t (a, b, c);
SHOW CREATE TABLE "t";
SHOW CREATE TABLE "^^t";
DROP TABLE t;

CREATE TABLE t (a, QUALIFIER INDEX a);
SHOW CREATE TABLE "t";
SHOW CREATE TABLE "^^t";
DROP TABLE t;
DROP TABLE '^^t';

CREATE TABLE t (a, b, c, QUALIFIER INDEX a, QUALIFIER INDEX b, QUALIFIER INDEX c);
SHOW CREATE TABLE "t";
SHOW CREATE TABLE "^^t";
DROP TABLE t;

CREATE TABLE t (a, b, INDEX a, QUALIFIER INDEX b);
SHOW CREATE TABLE t;
DESCRIBE TABLE t;
CREATE TABLE t2 LIKE t;
SHOW CREATE TABLE t2;
DESCRIBE TABLE t2;
DROP TABLE t2;
DROP TABLE t;

CREATE TABLE t (a, b, c, QUALIFIER INDEX a, INDEX a, QUALIFIER INDEX b, INDEX b);
SHOW CREATE TABLE "t";
SHOW CREATE TABLE "^t";
SHOW CREATE TABLE "^^t";
RENAME TABLE t TO r;
SHOW CREATE TABLE "r";
SHOW CREATE TABLE "^r";
SHOW CREATE TABLE "^^r";
DROP TABLE r;
DROP TABLE "^r";
DROP TABLE "^^r";

CREATE TABLE t (a, QUALIFIER INDEX a, INDEX a, b, QUALIFIER INDEX b, INDEX b);
INSERT INTO t VALUES ("rowa0", "a", "cella0:");
INSERT INTO t VALUES ("rowa1", "a:foo", "cella0:foo");
INSERT INTO t VALUES ("rowa2", "a:bar", "cella0:bar");
INSERT INTO t VALUES ("rowb0", "b", "cellb0:");
INSERT INTO t VALUES ("rowb1", "b:foo", "cellb0:foo");
INSERT INTO t VALUES ("rowb2", "b:bar", "cellb0:bar");
SELECT * FROM t;
SELECT * FROM "^t";
SELECT * FROM "^^t";
DROP TABLE t;

# qualifier tests
CREATE TABLE t (a, b, c, QUALIFIER INDEX a, QUALIFIER INDEX b);
INSERT INTO t VALUES ("row1", "a:foo1", "cell1");
INSERT INTO t VALUES ("row2", "b:bar1", "cell2");
INSERT INTO t VALUES ("row3", "c", "cell3");
SELECT * FROM "t";
SELECT * FROM "^^t";
INSERT INTO t VALUES ("row1", "a:foo2", "cell1");
INSERT INTO t VALUES ("row2", "b:bar2", "cell2");
INSERT INTO t VALUES ("row3", "c", "cell3");
SELECT * FROM "t";
SELECT * FROM "^^t";
INSERT INTO t VALUES ("row11", "a:foo3", "cell11");
INSERT INTO t VALUES ("row12", "b:bar3", "cell12");
INSERT INTO t VALUES ("row13", "c", "cell13");
SELECT * FROM "t";
SELECT * FROM "^^t";
INSERT INTO t VALUES ("row15", "a:foo4", "cell15");
INSERT INTO t VALUES ("row16", "b:bar4", "cell16");
INSERT INTO t VALUES ("row17", "c", "cell17");
SELECT * FROM "t";
SELECT * FROM "^^t";
INSERT INTO t VALUES ("row\t25", "a:foo5", "cell\t25");
INSERT INTO t VALUES ("row\t26", "b:bar5", "cell\t26");
INSERT INTO t VALUES ("row\t27", "c", "cell\t27");
SELECT * FROM "t";
SELECT * FROM "^^t";

DROP TABLE t;
CREATE TABLE t(a, INDEX a);
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa0", "a:q0", "cella0");
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa1", "a:q0", "cella1");
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa2", "a:q0", "cella2");
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa3", "a:q0", "cella3");
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa4", "a:q0", "cella4");
INSERT INTO t VALUES ("2011-11-10 08:09:29", "rowa10", "a:q0", "mella10");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa5", "a:q1", "cella5");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa6", "a:q1", "cella6");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa7", "a:q1", "cella7");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa8", "a:q1", "cella8");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa9", "a:q1", "cella9");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa11", "a:q1", "mella11");
SELECT a:/^q/ FROM t;
SELECT a:q0 FROM t;
SELECT unknown FROM t;
SELECT a:q0, a:q1 FROM t;

SELECT a:q0 FROM t WHERE "2011-11-10 00:00:00" < TIMESTAMP < "2011-11-11 00:00:00" DISPLAY_TIMESTAMPS;
SELECT a:q1 FROM t WHERE "2011-11-10 00:00:00" < TIMESTAMP < "2011-11-11 00:00:00" DISPLAY_TIMESTAMPS;

SELECT a:q0 FROM t WHERE (ROW < "rowa3" OR ROW > "rowa7");
SELECT a:q0 FROM t WHERE (ROW < "rowa3");
SELECT a:q0 FROM t WHERE ("rowa10" < ROW <= "rowa3");
SELECT a:q0 FROM t WHERE ("rowa10" <= ROW <= "rowa3");
SELECT a:q0 FROM t WHERE ("rowa10" <= ROW < "rowa3");
SELECT a:q0 FROM t WHERE ("rowa10" <= ROW < "rowa3" OR ROW < "rowa9");
SELECT a:q0 FROM t WHERE ("rowa1" <= ROW < "rowa3" OR ROW <= "rowa9");
SELECT a:q0 FROM t WHERE ("rowa1" <= ROW < "rowa3" OR ROW < "rowa9x");
SELECT a:q0 FROM t WHERE (ROW < "00");
SELECT a:q0 FROM t WHERE ROW REGEXP "rowa";
SELECT a:q0 FROM t WHERE ROW REGEXP "^rowa";
SELECT a:q0 FROM t WHERE ROW REGEXP "^rowa$";
SELECT a:q0 FROM t WHERE ROW REGEXP "rowa$";
SELECT a:q0 FROM t WHERE ROW REGEXP "3$";
SELECT a:q0 FROM t WHERE ROW REGEXP "\d$";

SELECT a:q0 FROM t WHERE (ROW < "rowa3" OR ROW > "rowa7");
SELECT a:q0 FROM t WHERE (ROW < "rowa3");
SELECT a:q0 FROM t WHERE ("rowa10" < ROW <= "rowa3");
SELECT a:q0 FROM t WHERE ("rowa10" <= ROW <= "rowa3");
SELECT a:q0 FROM t WHERE ("rowa10" <= ROW < "rowa3");
SELECT a:q0 FROM t WHERE ("rowa10" <= ROW < "rowa3" OR ROW < "rowa9");
SELECT a:q0 FROM t WHERE ("rowa1" <= ROW < "rowa3" OR ROW <= "rowa9");
SELECT a:q0 FROM t WHERE ("rowa1" <= ROW < "rowa3" OR ROW < "rowa9x");
SELECT a:q0 FROM t WHERE (ROW < "00");
SELECT a:q0 FROM t WHERE ROW REGEXP "rowa";
SELECT a:q0 FROM t WHERE ROW REGEXP "^rowa";
SELECT a:q0 FROM t WHERE ROW REGEXP "^rowa$";
SELECT a:q0 FROM t WHERE ROW REGEXP "rowa$";
SELECT a:q0 FROM t WHERE ROW REGEXP "3$";
SELECT a:q0 FROM t WHERE ROW REGEXP "\d$";

SELECT a:q0, a:q1 FROM t WHERE ROW REGEXP "\d$";
SELECT a:q1, a:q2 FROM t WHERE ROW REGEXP "\d$";
SELECT a:q0, a:q1, a:q2 FROM t WHERE ROW REGEXP "\d$";

INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa12", "a:q1", "cella9xx");
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa13", "a:q1", "cella9xy");
SELECT a:q1 FROM t WHERE a:* = "cella9";
SELECT a:q1 FROM t WHERE a:* =^ "cella9";
SELECT a:q1 FROM t WHERE VALUE REGEXP "x";
SELECT a:q1 FROM t WHERE VALUE REGEXP "x$";

INSERT INTO t VALUES ("rowa3", "a:q0", "cella4");
SELECT a:q0 FROM t;
SELECT a:q0 FROM t KEYS_ONLY;

INSERT INTO t VALUES ("rowa3", "a:q0", "cella5");
SELECT a:q0 FROM t REVS = 1;
SELECT a:q0 FROM t REVS = 2;
SELECT a:q0 FROM t REVS = 3;
SELECT a:q0 FROM t CELL_LIMIT = 1;
SELECT a:q0 FROM t CELL_LIMIT_PER_FAMILY = 1;
SELECT a:q0 FROM t CELL_LIMIT_PER_FAMILY = 2;
SELECT a:q0 FROM t CELL_LIMIT_PER_FAMILY = 3;
SELECT a:q0 FROM t LIMIT = 1;

SELECT a:q0 FROM t OFFSET 2 LIMIT 3;
SELECT a:q0 FROM t OFFSET 3 LIMIT 3;
SELECT a:q0 FROM t OFFSET 2 CELL_LIMIT 3;
SELECT a:q0 FROM t CELL_OFFSET 2 LIMIT 3;
SELECT a:q0 FROM t CELL_OFFSET 5 CELL_LIMIT 1;

DELETE * FROM t WHERE ROW = "rowa2";
SELECT a:q0 FROM t WHERE a:* =^ "c";
SELECT a:q0 FROM t WHERE a:* =^ "c" RETURN_DELETES;
SELECT a:q0 FROM t WHERE a:* =^ "c" RETURN_DELETES KEYS_ONLY;

# negative test - index doesn't return any result
SELECT a:q0 FROM t WHERE a =^ "x";
SELECT a:q3 FROM t;
# negative test - index returns result but they're skipped in
# IndexResultCallback
SELECT a:q0 FROM t WHERE ROW < "a";
# negative test - index returns result but they're outdated
SELECT a:q0 FROM t WHERE VALUE REGEXP "^cella2";
SELECT a:q0 FROM t WHERE a =^ "cella2";

# overwrite a value, make sure that the index still works
INSERT INTO t VALUES ("2011-11-11 08:09:29", "rowa13", "a:q1", "cella9xz");
SELECT a:q1 FROM t WHERE VALUE REGEXP "^cella9";
SELECT a:q1 FROM t WHERE a:* =^ "cella9";

INSERT INTO t VALUES ("rowa20", "a", "cella20");
SELECT a FROM t WHERE ROW = "rowa20";

# test for qualifiers with prefix
INSERT INTO t VALUES ("rowa10", "a:q0", "cella10");
INSERT INTO t VALUES ("rowa11", "a:q01", "cella11");
INSERT INTO t VALUES ("rowa12", "a:q012", "cella12");
INSERT INTO t VALUES ("rowa13", "a:q0123", "cella13");
INSERT INTO t VALUES ("rowa14", "a:^q0123", "cella14");
SELECT a FROM t;
SELECT a:q0 FROM t;
SELECT a:q01 FROM t;
SELECT a:^q01 FROM t;
SELECT a:q012 FROM t;
SELECT a:^q012 FROM t;
SELECT a:^"^" FROM t;

DROP TABLE t;
CREATE TABLE t(a, QUALIFIER INDEX a, b);
INSERT INTO t VALUES ("rowa0", "a:q0", "cella0");
INSERT INTO t VALUES ("rowa1", "a:q0", "cella1");
INSERT INTO t VALUES ("rowa2", "a:q0", "cella2");
INSERT INTO t VALUES ("rowa10", "a:q0", "cella10");
INSERT INTO t VALUES ("rowa11", "a:q01", "cella11");
INSERT INTO t VALUES ("rowa12", "a:q012", "cella12");
INSERT INTO t VALUES ("rowa13", "a:q0123", "cella13");
INSERT INTO t VALUES ("rowa14", "a:^q0123", "cella14");
SELECT a FROM t;
SELECT a:q0 FROM t;
SELECT a:q01 FROM t;
SELECT a:^q01 FROM t;
SELECT a:q012 FROM t;
SELECT a:^q012 FROM t;
SELECT a:^"^" FROM t;

SELECT * FROM t WHERE a:* = "cella0";
SELECT b FROM t WHERE a:* = "cella0";
SELECT a FROM t WHERE a:* = "cella0";
SELECT a FROM t WHERE a:* = "cella11";
SELECT a FROM t WHERE a:* = "cell";
SELECT a FROM t WHERE a:* =^ "cell";
SELECT a FROM t WHERE a:* =^ "c";
SELECT a FROM t WHERE a:* =^ "";
SELECT a FROM t WHERE a:* =^ "xas";

# drop one index and make sure that the queries still work
# TODO re-enable this when ALTER TABLE is implemented
# ALTER TABLE t DROP (INDEX idxb);
# SELECT a FROM t WHERE a =^ "cell" LIMIT 8;
# SELECT b FROM t WHERE a =^ "cell" LIMIT 8;
# SELECT a, b FROM t WHERE a =^ "cell" LIMIT 8;
# SELECT b, a FROM t WHERE a =^ "cell" LIMIT 8;
# SELECT * FROM t WHERE a =^ "cell" LIMIT 8;
# SELECT * FROM t WHERE ROW < "row05" AND a =^ "cell";
# SELECT a FROM t WHERE ROW < "row05" AND a =^ "cell";

# verify that /tmp is empty
USE "/tmp";
GET LISTING;

# issue 823
CREATE TABLE t (data MAX_VERSIONS = 1, join MAX_VERSIONS = 1, QUALIFIER INDEX join);
INSERT INTO t VALUES ('r1', 'join:a1', 'value 1');
INSERT INTO t VALUES ('r2', 'join:a2', 'value 2');
INSERT INTO t VALUES ('r3', 'join:b1', 'value 3');
SELECT join:a1 FROM t WHERE VALUE REGEXP 'v';

# issue 989
CREATE TABLE badidea (c COUNTER, INDEX c);
CREATE TABLE badidea (c COUNTER, QUALIFIER INDEX c);

# More Index testing
DROP TABLE IF EXISTS User;
CREATE TABLE User (name, address, age, index name, index age);
INSERT INTO User VALUES ('2014-01-17 03:41:29', '123456789', 'name', 'Doug'),
('2014-01-17 03:41:29', '123456789', 'address', '2999 Canyon Rd.'),
('2014-01-17 03:41:29', '123456789', 'age', '45');
INSERT INTO User VALUES ('2014-01-17 03:41:30', '234567890', 'name', 'Joe'),
('2014-01-17 03:41:30', '234567890', 'address', '123 Main St.'),
('2014-01-17 03:41:30', '234567890', 'age', '57');
INSERT INTO User VALUES ('2014-01-17 03:41:31', '345678901', 'name', 'Doug'),
('2014-01-17 03:41:31', '345678901', 'address', '234 Maple St.'),
('2014-01-17 03:41:31', '345678901', 'age', '12');
SELECT * FROM User WHERE address="2999 Canyon Rd.";
SELECT * FROM User WHERE name=^"D";
SELECT * FROM User WHERE name="Doug";
SELECT age FROM User WHERE name="Doug";
SELECT * FROM User WHERE name="Doug" AND age="45";
SELECT address FROM User WHERE name="Doug" AND TIMESTAMP < "2014-01-17 03:41:30";
SELECT address FROM User WHERE name="Doug" AND TIMESTAMP > "2014-01-17 03:41:30";

# Index tests with qualifier
DROP TABLE IF EXISTS User;
CREATE TABLE User (age, infoA, infoB, index infoA);
INSERT INTO User VALUES
('2014-01-17 03:41:29', '123456789', 'infoA:first', 'Bobby'),
('2014-01-17 03:41:29', '123456789', 'infoA:last', 'Jones'),
('2014-01-17 03:41:29', '123456789', 'age', '45'),
('2014-01-17 03:41:30', '234567890', 'infoA:first', 'Ricky'),
('2014-01-17 03:41:30', '234567890', 'infoA:last', 'Bobby'),
('2014-01-17 03:41:30', '234567890', 'age', '57'),
('2014-01-17 03:41:31', '345678901', 'infoB:first', 'Ricky'),
('2014-01-17 03:41:31', '345678901', 'infoB:last', 'Bobby'),
('2014-01-17 03:41:31', '345678901', 'infoB:child', 'Ronnie'),
('2014-01-17 03:41:31', '345678901', 'age', '83'),
('2014-01-17 03:41:29', '345678902', 'infoA:first', 'Boris'),
('2014-01-17 03:41:29', '345678902', 'infoA:last', 'Yeltsin'),
('2014-01-17 03:41:31', '345678902', 'infoB:child', 'Roland'),
('2014-01-17 03:41:31', '345678902', 'infoB:brother', 'Roland'),
('2014-01-17 03:41:29', '345678902', 'age', '78');
SELECT age FROM User WHERE infoA:last="Bobby";
SELECT * FROM User WHERE infoA:first = 'Bobby';
SELECT * FROM User WHERE infoA:first =^ 'Bo';
SELECT * FROM User WHERE infoB:child = 'Ronnie';
SELECT * FROM User WHERE infoB:child =^ 'Ro';

# More Index tests
DROP TABLE IF EXISTS products;
CREATE TABLE products (
  title,
  section,
  info,
  category,
  INDEX section,
  INDEX info,
  QUALIFIER INDEX info,
  QUALIFIER INDEX category
);
LOAD DATA INFILE 'indices_test_products.tsv' INTO TABLE products;
SELECT title FROM products WHERE section = "books";
SELECT title FROM products WHERE info:actor = "Jack Nicholson";
SELECT title,info:publisher FROM products WHERE info:publisher =^ 'Addison-Wesley';
SELECT title,info:publisher FROM products WHERE info:publisher =~ /^Addison-Wesley/;
SELECT title,info:author FROM products WHERE info:author =~ /^Stephen [PK]/;
SELECT title,info:author FROM products WHERE Regexp(info:author, '^Stephen [PK]');
SELECT title FROM products WHERE Exists(info:studio);
SELECT title FROM products WHERE Exists(category:/^\/Movies/);
SELECT title FROM products WHERE Exists(category:/^\/Books.*Programming/);
SELECT title FROM products WHERE info:author =~ /^Stephen P/ OR info:publisher =^ "Anchor";
SELECT title FROM products WHERE info:author =~ /^Stephen P/ OR info:publisher =~ /^Anchor/;
SELECT title FROM products WHERE info:author =~ /^Stephen [PK]/ AND info:publisher =^ "Anchor";
SELECT title FROM products WHERE info:author =~ /^Stephen [PK]/ AND info:publisher =~ /^Anchor/;
SELECT * FROM "^products" INTO FILE "products-value-index-before.tsv" DISPLAY_TIMESTAMPS;
SELECT * FROM "^^products" INTO FILE "products-qualifier-index-before.tsv" DISPLAY_TIMESTAMPS;
REBUILD INDICES products;
SELECT * FROM "^products" INTO FILE "products-value-index-after.tsv" DISPLAY_TIMESTAMPS;
SELECT * FROM "^^products" INTO FILE "products-qualifier-index-after.tsv" DISPLAY_TIMESTAMPS;
