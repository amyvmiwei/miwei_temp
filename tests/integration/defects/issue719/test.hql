
USE "/";

DROP TABLE IF EXISTS TestIssue719;

CREATE TABLE TestIssue719 (
  dtg, status, callsign, origin, destination, origin_time, 
  destination_time, longitude, latitude, type, altitude, speed, heading
);

LOAD DATA INFILE ROW_KEY_COLUMN=dtg "test.tsv" INTO TABLE TestIssue719;
SELECT * FROM TestIssue719;

