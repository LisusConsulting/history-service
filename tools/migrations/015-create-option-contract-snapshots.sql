-- 013 — option_contract_snapshots hypertable.
--
-- Phase 4 of the data-provider centralization (2026-05-09). Holds per-minute
-- intraday option chain snapshots that mbd-data-provider rolls over here at
-- end-of-day. Polygon's snapshot endpoint serves CURRENT only and does not
-- time-travel — these rows are the only persistent record of intraday
-- chain state for replay-with-different-config and post-hoc analysis.
--
-- Schema mirrors mbd-data-provider's hot-tier table 1:1 so the rollover is
-- a straight INSERT...SELECT across DBs (or via gRPC bulk-write).
--
-- Hypertable partitioned on ts_minute so daily/weekly queries hit a small
-- chunk set; the (ts_minute, contract_ticker) primary key gives O(log n)
-- single-contract lookups within a chunk.

CREATE TABLE IF NOT EXISTS option_contract_snapshots (
    ts_minute        TIMESTAMPTZ    NOT NULL,
    underlying       TEXT           NOT NULL,
    contract_ticker  TEXT           NOT NULL,
    contract_type    TEXT,                                  -- 'call' | 'put'
    strike           DECIMAL(18,4),
    expiration_date  DATE,
    bid              DECIMAL(18,4),
    ask              DECIMAL(18,4),
    bid_size         INTEGER,
    ask_size         INTEGER,
    last_trade       DECIMAL(18,4),
    iv               DECIMAL(10,6),
    delta            DECIMAL(10,6),
    gamma            DECIMAL(12,8),
    theta            DECIMAL(12,6),
    vega             DECIMAL(12,6),
    open_interest    BIGINT,
    day_volume       BIGINT,
    -- 2026-05-09: nanosecond-epoch timestamp of the last NBBO update from
    -- Polygon. MBD's stale-quote gate (9:30 ET cold-open defense) needs
    -- this — a snapshot's bid/ask might be stale by hours but the row
    -- itself looks valid. Compare (now - last_updated_ns) > 60s to detect.
    last_updated_ns  BIGINT,
    captured_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    rolled_over_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()  -- when row landed here from data-provider
);

-- Idempotent column add for tables created before this column existed.
ALTER TABLE option_contract_snapshots
    ADD COLUMN IF NOT EXISTS last_updated_ns BIGINT;

-- Hypertable on ts_minute. 1-day chunks (default for timescaledb).
SELECT create_hypertable(
    'option_contract_snapshots', 'ts_minute',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

-- Composite key allows duplicate rolls (same row, second EOD) to upsert harmlessly.
CREATE UNIQUE INDEX IF NOT EXISTS uq_option_contract_snapshots
    ON option_contract_snapshots (ts_minute, contract_ticker);

-- Whole-chain at a minute lookup: ($underlying, $ts) → all contracts at minute.
CREATE INDEX IF NOT EXISTS idx_option_contract_snapshots_underlying_ts
    ON option_contract_snapshots (underlying, ts_minute DESC);
