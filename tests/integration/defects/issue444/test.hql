
USE "/";

DROP TABLE IF EXISTS TestIssue444;

CREATE TABLE TestIssue444 (
  anon_id, query, item, click_url
);

LOAD DATA INFILE "test.tsv" INTO TABLE TestIssue444;
SELECT * FROM TestIssue444;

DROP TABLE IF EXISTS TestIssue444;

CREATE TABLE TestIssue444 (
  anon_id, item, click_url
);

LOAD DATA INFILE "test.tsv" INTO TABLE TestIssue444;
SELECT * FROM TestIssue444;

LOAD DATA INFILE IGNORE_UNKNOWN_COLUMNS "test.tsv" INTO TABLE TestIssue444;
SELECT * FROM TestIssue444;
