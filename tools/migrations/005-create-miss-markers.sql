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

-- NBBO: point-keyed misses (ticker, ts).
CREATE TABLE IF NOT EXISTS historical_options_quotes_misses (
  ticker     VARCHAR(50)  NOT NULL,
  ts         TIMESTAMPTZ  NOT NULL,
  reason     TEXT,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, ts)
);

-- Chains: date-keyed misses (symbol, as_of_date).
CREATE TABLE IF NOT EXISTS historical_options_chains_misses (
  symbol      VARCHAR(10)  NOT NULL,
  as_of_date  DATE         NOT NULL,
  reason      TEXT,
  fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (symbol, as_of_date)
);
CREATE INDEX IF NOT EXISTS idx_historical_options_chains_misses_lookup
  ON historical_options_chains_misses (symbol, as_of_date);

-- Macro: date-keyed misses (series_id, observation_date).
CREATE TABLE IF NOT EXISTS macro_data_misses (
  series_id        VARCHAR(20)  NOT NULL,
  observation_date DATE         NOT NULL,
  reason           TEXT,
  fetched_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (series_id, observation_date)
);
CREATE INDEX IF NOT EXISTS idx_macro_data_misses_lookup
  ON macro_data_misses (series_id, observation_date);
