-- 001 — historical_bars hypertable (TimescaleDB).
-- Lifted verbatim from MBD docker/postgres/init.sql to keep dev-data-import
-- compatible: Phase 3's data migration is a pg_dump | pg_restore from the
-- existing dev DB and any schema drift would corrupt the restore.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS historical_bars (
  symbol VARCHAR(10) NOT NULL,
  timeframe VARCHAR(10) NOT NULL, -- 'day', '1min', '5min'
  timestamp TIMESTAMPTZ NOT NULL,
  open DECIMAL(18,4) NOT NULL,
  high DECIMAL(18,4) NOT NULL,
  low DECIMAL(18,4) NOT NULL,
  close DECIMAL(18,4) NOT NULL,
  volume DECIMAL(18,2) NOT NULL,
  vwap DECIMAL(18,4),
  trade_count INT
);

SELECT create_hypertable('historical_bars', 'timestamp', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS uq_bars_symbol_timeframe_timestamp
  ON historical_bars (symbol, timeframe, timestamp);
CREATE INDEX IF NOT EXISTS idx_bars_symbol_timeframe
  ON historical_bars (symbol, timeframe, timestamp DESC);
