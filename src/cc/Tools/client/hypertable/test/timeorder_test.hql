CREATE NAMESPACE "/timeorder";
USE "/timeorder";
DROP TABLE IF EXISTS timeorder_test;

# negative tests failing with syntax errors
CREATE TABLE timeorder_test (cf1 TIME_ORDER, cf2, cf3);
CREATE TABLE timeorder_test (cf1 TIME_ORDER blah, cf2, cf3);
CREATE TABLE timeorder_test (cf1 TIME_ORDER ASC TIME_ORDER ASC, cf2, cf3);
CREATE TABLE timeorder_test (cf1 TIME_ORDER ASC TIME_ORDER DESC, cf2, cf3);
CREATE TABLE timeorder_test (cf1 TIME_ORDER DESC TIME_ORDER DESC, cf2, cf3);
CREATE TABLE timeorder_test (cf1 TIME_ORDER DESC TIME_ORDER ASC, cf2, cf3);

# TIME_ORDER ASC is the default
CREATE TABLE timeorder_test (cf1 TIME_ORDER ASC, cf2);
INSERT INTO timeorder_test VALUES ('r1', 'cf1', 'val1.1');
INSERT INTO timeorder_test VALUES ('r1', 'cf2', 'val1.2');
INSERT INTO timeorder_test VALUES ('r1', 'cf1', 'val2.1');
INSERT INTO timeorder_test VALUES ('r1', 'cf2', 'val2.2');
INSERT INTO timeorder_test VALUES ('r1', 'cf1', 'val3.1');
INSERT INTO timeorder_test VALUES ('r1', 'cf2', 'val3.2');
INSERT INTO timeorder_test VALUES ('r1', 'cf1', 'val4.1');
INSERT INTO timeorder_test VALUES ('r1', 'cf2', 'val4.2');
INSERT INTO timeorder_test VALUES ('r1', 'cf1', 'val5.1');
INSERT INTO timeorder_test VALUES ('r1', 'cf2', 'val5.2');
SELECT * FROM timeorder_test;

# make sure that the schema is persistent
DROP TABLE IF EXISTS timeorder_test;
CREATE TABLE timeorder_test (cf1 TIME_ORDER DESC, cf2);
DESCRIBE TABLE timeorder_test;
SHOW CREATE TABLE timeorder_test;

# insert more data, verify
INSERT INTO timeorder_test VALUES ('r1', 'cf1', 'val1');
INSERT INTO timeorder_test VALUES ('r1', 'cf1', 'val2');
INSERT INTO timeorder_test VALUES ('r1', 'cf1', 'val3');
INSERT INTO timeorder_test VALUES ('r1', 'cf1', 'val4');
INSERT INTO timeorder_test VALUES ('r1', 'cf1', 'val5');
INSERT INTO timeorder_test VALUES ('r1', 'cf2', 'val1');
INSERT INTO timeorder_test VALUES ('r1', 'cf2', 'val2');
INSERT INTO timeorder_test VALUES ('r1', 'cf2', 'val3');
INSERT INTO timeorder_test VALUES ('r1', 'cf2', 'val4');
INSERT INTO timeorder_test VALUES ('r1', 'cf2', 'val5');
SELECT * FROM timeorder_test;

# insert/verify but specify the timestamps
DROP TABLE IF EXISTS timeorder_test;
CREATE TABLE timeorder_test (cf1 TIME_ORDER DESC, cf2);
INSERT INTO timeorder_test VALUES ("2011-10-10 04:45:50.000000000", 'r1', 'cf1', 'val1');
INSERT INTO timeorder_test VALUES ("2011-10-10 04:45:51.000000000", 'r1', 'cf1', 'val2');
INSERT INTO timeorder_test VALUES ("2011-10-10 04:45:52.000000000", 'r1', 'cf1', 'val3');
INSERT INTO timeorder_test VALUES ("2011-10-10 04:45:53.000000000", 'r1', 'cf1', 'val4');
INSERT INTO timeorder_test VALUES ("2011-10-10 04:45:54.000000000", 'r1', 'cf1', 'val5');
INSERT INTO timeorder_test VALUES ("2011-10-10 04:45:50.000000000", 'r1', 'cf2', 'val1');
INSERT INTO timeorder_test VALUES ("2011-10-10 04:45:51.000000000", 'r1', 'cf2', 'val2');
INSERT INTO timeorder_test VALUES ("2011-10-10 04:45:52.000000000", 'r1', 'cf2', 'val3');
INSERT INTO timeorder_test VALUES ("2011-10-10 04:45:53.000000000", 'r1', 'cf2', 'val4');
INSERT INTO timeorder_test VALUES ("2011-10-10 04:45:54.000000000", 'r1', 'cf2', 'val5');
SELECT * FROM timeorder_test DISPLAY_TIMESTAMPS;

