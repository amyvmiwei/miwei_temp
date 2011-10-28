
USE "/";

DROP TABLE IF EXISTS TestIssue720;

CREATE TABLE TestIssue720 (
  dtg, status, callsign, origin, destination, origin_time, 
  destination_time, longitude, latitude, type, altitude, speed, heading
);

LOAD DATA INFILE ROW_KEY_COLUMN=dtg "test.tsv" INTO FILE "test.output";

