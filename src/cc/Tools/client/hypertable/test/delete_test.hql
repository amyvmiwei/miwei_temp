USE '/';
DROP TABLE IF EXISTS "delete_test";
CREATE TABLE "delete_test" (column);
LOAD DATA INFILE 'delete_test.tsv' INTO TABLE "delete_test";
SELECT * FROM "delete_test" INTO FILE 'delete_test_output.tsv';

