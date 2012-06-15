
USE "/";

DROP TABLE IF EXISTS TestIssue866;

CREATE TABLE TestIssue866 (
  title MAX_VERSIONS=1,
  content MAX_VERSIONS=1,
  name MAX_VERSIONS=1,
  url MAX_VERSIONS=1, INDEX url,
  site MAX_VERSIONS=1,
  metadata MAX_VERSIONS=1, QUALIFIER INDEX metadata,
  ACCESS GROUP default (title, content, name, url, site, metadata)
);

LOAD DATA INFILE "dump.tsv.gz" INTO TABLE TestIssue866;
SELECT metadata:filename FROM TestIssue866;

