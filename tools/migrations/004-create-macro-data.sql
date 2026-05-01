-- 004 — FRED macro observations.
-- One row per (series_id, observation_date). Not a hypertable — daily
-- cardinality is low enough that a regular b-tree is fine.
CREATE TABLE IF NOT EXISTS macro_data (
  series_id VARCHAR(20) NOT NULL,
  observation_date DATE NOT NULL,
  value DECIMAL(18,6)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_series_date
  ON macro_data (series_id, observation_date);
CREATE INDEX IF NOT EXISTS idx_macro_series
  ON macro_data (series_id, observation_date DESC);
