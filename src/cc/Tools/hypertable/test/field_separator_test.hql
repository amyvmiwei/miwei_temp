USE '/';
CREATE NAMESPACE 'fs_test';
USE 'fs_test';
DROP TABLE IF EXISTS first;
CREATE TABLE first (column);
DROP TABLE IF EXISTS second;
CREATE TABLE second (column);
LOAD DATA INFILE 'field_separator_test.tsv' INTO TABLE first;
SELECT * FROM first INTO FILE 'field_separator_test.csv' FS=',';
LOAD DATA INFILE FS=',' 'field_separator_test.csv' INTO TABLE second;
SELECT * FROM second INTO FILE 'field_separator_test_output.tsv';

