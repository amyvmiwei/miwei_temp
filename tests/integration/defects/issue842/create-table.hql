USE '/';
DROP TABLE IF EXISTS metrics;
CREATE TABLE metrics (
  metric,
  QUALIFIER INDEX metric,
  ACCESS GROUP default (metric)
);
