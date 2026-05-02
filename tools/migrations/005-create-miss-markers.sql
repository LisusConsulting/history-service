-- 005 — Miss markers for cold-start determinism.
--
-- Every on-demand fetcher consults its corresponding miss-markers table
-- BEFORE issuing an upstream call. A row here means "we already tried,
-- upstream had no data" (holiday, halt, weekend, pre-listing, etc.) —
-- subsequent backtest replays skip the re-fetch. Without these, every
-- cold-start backtest pays the full Polygon / FRED round-trip per
-- known-empty key.

-- Bars: range-keyed misses (symbol, timeframe, [range_from, range_to]).
CREATE TABLE IF NOT EXISTS historical_bars_misses (
  symbol      VARCHAR(10)  NOT NULL,
  timeframe   VARCHAR(10)  NOT NULL,
  range_from  TIMESTAMPTZ  NOT NULL,
  range_to    TIMESTAMPTZ  NOT NULL,
  reason      TEXT,
  fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (symbol, timeframe, range_from, range_to)
);
CREATE INDEX IF NOT EXISTS idx_historical_bars_misses_lookup
  ON historical_bars_misses (symbol, timeframe, range_from, range_to);

-- NBBO: range-keyed misses (ticker, [range_from, range_to]).
-- Originally point-keyed (ticker, ts); migrated to range-shape in 009
-- so the intra-range gap-detection path can store one row per
-- contiguous missing minute-run (brief 2026-05-02). Fresh-volume inits
-- create the new shape directly so 009 is a no-op on fresh DBs and only
-- does real work on existing dev/paper volumes.
CREATE TABLE IF NOT EXISTS historical_options_quotes_misses (
  ticker      VARCHAR(50)  NOT NULL,
  range_from  TIMESTAMPTZ  NOT NULL,
  range_to    TIMESTAMPTZ  NOT NULL,
  reason      TEXT,
  fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, range_from, range_to)
);
CREATE INDEX IF NOT EXISTS idx_historical_options_quotes_misses_lookup
  ON historical_options_quotes_misses (ticker, range_from, range_to);

-- Chains: range-keyed misses (symbol, [range_from, range_to]).
-- Originally point-keyed (symbol, as_of_date); migrated to range-shape in
-- 010 so the intra-range gap-detection path can store one row per
-- contiguous run of missing trading days (brief 2026-05-02). Fresh-volume
-- inits create the new shape directly so 010 is a no-op on fresh DBs and
-- only does real work on existing dev/paper volumes.
CREATE TABLE IF NOT EXISTS historical_options_chains_misses (
  symbol      VARCHAR(10)  NOT NULL,
  range_from  DATE         NOT NULL,
  range_to    DATE         NOT NULL,
  reason      TEXT,
  fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (symbol, range_from, range_to)
);
CREATE INDEX IF NOT EXISTS idx_historical_options_chains_misses_lookup
  ON historical_options_chains_misses (symbol, range_from, range_to);

-- Macro: range-keyed misses (series_id, [range_from, range_to]).
-- Originally point-keyed (series_id, observation_date); migrated to
-- range-shape in 011 so the intra-range gap-detection path can store
-- one row per contiguous run of missing publication boundaries (brief
-- 2026-05-02). Fresh-volume inits create the new shape directly so 011
-- is a no-op on fresh DBs and only does real work on existing
-- dev/paper volumes.
CREATE TABLE IF NOT EXISTS macro_data_misses (
  series_id   VARCHAR(20)  NOT NULL,
  range_from  DATE         NOT NULL,
  range_to    DATE         NOT NULL,
  reason      TEXT,
  fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (series_id, range_from, range_to)
);
CREATE INDEX IF NOT EXISTS idx_macro_data_misses_lookup
  ON macro_data_misses (series_id, range_from, range_to);
