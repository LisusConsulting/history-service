-- 006 — cache_stats time-series.
-- Periodic snapshots of the in-process cache-stats counters. Persisted so
-- restarts don't zero the operability dashboard, and so we can plot cache
-- hit ratios / fetch volumes / latency-percentiles over multi-day windows.
--
-- The service writes one row per data class per snapshot interval (default
-- 60 s). Aggregate-across-classes views are computed at read time.

CREATE TABLE IF NOT EXISTS cache_stats (
  recorded_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  data_class         VARCHAR(20) NOT NULL,           -- 'bars' | 'nbbo' | 'chains' | 'macro' | 'all'
  total_requests     BIGINT      NOT NULL DEFAULT 0,
  cache_hits         BIGINT      NOT NULL DEFAULT 0,
  polygon_fetches    BIGINT      NOT NULL DEFAULT 0, -- upstream calls (polygon | fred | etc.)
  miss_markers       BIGINT      NOT NULL DEFAULT 0, -- count of recorded miss-marker hits
  in_flight_count_p50 INT        NOT NULL DEFAULT 0,
  in_flight_count_p95 INT        NOT NULL DEFAULT 0,
  in_flight_count_p99 INT        NOT NULL DEFAULT 0,
  latency_ms_p50     DOUBLE PRECISION NOT NULL DEFAULT 0,
  latency_ms_p95     DOUBLE PRECISION NOT NULL DEFAULT 0,
  latency_ms_p99     DOUBLE PRECISION NOT NULL DEFAULT 0,
  PRIMARY KEY (recorded_at, data_class)
);

CREATE INDEX IF NOT EXISTS idx_cache_stats_class_recorded
  ON cache_stats (data_class, recorded_at DESC);
