
USE "/";

DROP TABLE IF EXISTS TestIssue873;

CREATE TABLE TestIssue873 (
  a COUNTER
);

INSERT INTO TestIssue873 VALUES ("r0", "a", "1");
INSERT INTO TestIssue873 VALUES ("r1", "a", "1");
SELECT * FROM TestIssue873;
INSERT INTO TestIssue873 VALUES ("r0", "a", "1");
SELECT * FROM TestIssue873;
INSERT INTO TestIssue873 VALUES ("r0", "a", "=1"), ("r1", "a", "=1");
SELECT * FROM TestIssue873;

