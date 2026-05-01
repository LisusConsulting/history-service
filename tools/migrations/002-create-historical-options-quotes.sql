-- 002 — historical_options_quotes hypertable.
-- Selective NBBO cache. ts is the REQUESTED timestamp (cache lookup key);
-- as_of_ts is the SIP timestamp the quote was actually published at —
-- distinguishes "today's open NBBO" from "yesterday's close stamped under
-- today's bar boundary" (see 9:30 mispricing analysis 2026-04-29).
CREATE TABLE IF NOT EXISTS historical_options_quotes (
  ticker            VARCHAR(50)  NOT NULL,
  ts                TIMESTAMPTZ  NOT NULL,
  as_of_ts          TIMESTAMPTZ,
  bid_price         DECIMAL(18,4),
  ask_price         DECIMAL(18,4),
  bid_size          INT,
  ask_size          INT,
  bid_exchange      INT,
  ask_exchange      INT,
  underlying_price  DECIMAL(18,4),
  fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('historical_options_quotes', 'ts', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS uq_options_quotes_ticker_ts
  ON historical_options_quotes (ticker, ts);
CREATE INDEX IF NOT EXISTS idx_options_quotes_ts
  ON historical_options_quotes (ts DESC);
ALTER TABLE historical_options_quotes ADD COLUMN IF NOT EXISTS as_of_ts TIMESTAMPTZ;
