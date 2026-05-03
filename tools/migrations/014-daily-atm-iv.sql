-- 014 — daily_atm_iv + daily_atm_iv_misses.
--
-- Wave A / PR 1 of the ATM-IV full historical coverage plan
-- (docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md).
--
-- One row per (underlying, trade_date) carrying the ATM-IV aggregate
-- consumed by MBD's IV-regime signal (currently sourced from
-- MBD-local `daily_atm_iv` — migrated to history-service via Wave D).
-- The aggregate is the average of `implied_volatility` across the
-- ATM-band snapshots for that day in `historical_options_snapshots`,
-- with `DISTINCT ON (ticker, date_trunc('day', snapshot_date))` to
-- normalize the cross-source cadence asymmetry described in the plan
-- (Concern G in Step 3): polygon_live rows are 5-min cadence (~78
-- intra-day samples), computed_bs rows are 1 row per (contract, day)
-- at EOD. Picking the latest row per (contract, day) means both
-- sources contribute one EOD row each, so the daily aggregate is
-- comparable across the live-vs-computed boundary.
--
-- Population paths:
--   * Wave B / PR 5 — gRPC GetDailyAtmIv reader (this PR).
--   * Wave C / PR 6 — daily 08:00 ET cron, computes yesterday's row.
--   * Wave C — backfill seeder mode --surface daily_atm_iv (extends
--     PR 3's seeder; lands in C+D dispatch).
--
-- gRPC reader: GetDailyAtmIv(symbol, from, to) → DailyAtmIvResponse
-- with a past-only guard (PR #13 / PastOnlyRangeValidator pattern).
-- Concurrency-safety: GapLockExecutor on the cron + seeder writers
-- (PR #24 pattern). Read path is plain SELECT — no upstream fetch on
-- miss; consumers run the seeder/cron first.

BEGIN;

-- ── daily_atm_iv ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_atm_iv (
  underlying_ticker VARCHAR(10)   NOT NULL,
  trade_date        DATE          NOT NULL,
  atm_iv            NUMERIC(10,6),
  contract_count    INT,
  fetched_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  PRIMARY KEY (underlying_ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_atm_iv_lookup
  ON daily_atm_iv (underlying_ticker, trade_date);

-- TimescaleDB hypertable on trade_date. 90-day chunks matches the
-- daily_options_flow chunking (migration 012) since the row cadence
-- and consumption pattern are identical: ~250 rows/year per symbol,
-- read in 60-day backtest windows.
SELECT create_hypertable('daily_atm_iv', 'trade_date',
  chunk_time_interval => INTERVAL '90 days',
  if_not_exists => TRUE);

-- ── daily_atm_iv_misses ───────────────────────────────────────────────
-- Range-shape miss markers, identical shape to daily_options_flow_misses.
-- A row marks a contiguous trading-day range where the seeder/cron
-- found no aggregable snapshots (e.g. NBBO empty for the entire
-- ATM-band that day, or solver convergence rate too low to publish a
-- row). Same RangeMarkerWriter helper applies unchanged (DATE-typed
-- bounds, 1-day adjacency).
CREATE TABLE IF NOT EXISTS daily_atm_iv_misses (
  underlying_ticker VARCHAR(10)  NOT NULL,
  range_from        DATE         NOT NULL,
  range_to          DATE         NOT NULL,
  reason            TEXT,
  fetched_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (underlying_ticker, range_from, range_to)
);

CREATE INDEX IF NOT EXISTS idx_daily_atm_iv_misses_lookup
  ON daily_atm_iv_misses (underlying_ticker, range_from, range_to);

COMMIT;
