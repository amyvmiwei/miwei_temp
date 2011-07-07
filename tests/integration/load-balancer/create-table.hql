use '/';
drop table if exists TEST_RS_METRICS;
CREATE TABLE TEST_RS_METRICS (
  server MAX_VERSIONS=336,
  range MAX_VERSIONS=24,
  'range_start_row' MAX_VERSIONS=1,
  'range_move' MAX_VERSIONS=1,
  ACCESS GROUP server (server),
  ACCESS GROUP range (range, 'range_start_row', 'range_move')
);
