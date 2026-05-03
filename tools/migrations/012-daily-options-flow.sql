-- 012 — daily_options_flow + daily_options_flow_misses.
--
-- Phase 5 follow-up. `daily_options_flow` is the per-(underlying, trade_date)
-- aggregated put/call flow score consumed by the BACKTEST engine for past-day
-- OptionsFlow signal evaluation. The LIVE engine never reads this table —
-- it computes flow on-the-fly per signal cycle via
-- OptionsAnalysisService.ComputeFlowScoreDetailed(currentChain).
--
-- Population paths:
--   1. Backfill (PR 2): seeder fans out per short-DTE contract over a date
--      range, fetches Polygon /v2/aggs daily volumes (concurrency 32), and
--      UPSERTs aggregated rows here.
--   2. Daily refresh (PR 3): an IHostedService fires at 08:00 ET each
--      weekday and rolls the previous trading day's flow for tracked
--      symbols.
--
-- gRPC reader (PR 1, this PR):
--   GetDailyOptionsFlow(symbol, from, to) → DailyOptionsFlowResponse
--   Past-only guard rejects today-or-later (consistent with Phase 1
--   pattern in PR #13 / PastOnlyRangeValidator).
--
-- Algorithm — same formula as MBD's deleted OptionsVolumeBackfillService
-- (recoverable from `git show 96ffcdd^:tools/DataIngestion/Services/OptionsVolumeBackfillService.cs`):
--   call_side = call_volume + 0.1 * call_oi
--   put_side  = put_volume  + 0.1 * put_oi
--   flow_score = clamp((1 - put_side / call_side) * 0.7, -1, +1)
--   put_call_ratio = put_side / call_side
--
-- ── OI caveat ────────────────────────────────────────────────────────
-- Polygon's /v2/aggs response does NOT include open_interest. Backfill
-- rows therefore carry call_oi=0 / put_oi=0; the live engine's identical
-- formula uses real OI from the same chain it just fetched. This
-- asymmetry is acceptable and matches the legacy MBD-side data shape
-- that shipped in production for years (PR-backed: same SQL aggregator
-- in OptionsVolumeBackfillService.AggregateFlowScoresAsync).
--
-- Schema notes:
--  * call_volume / put_volume are BIGINT (sum across short-DTE contracts
--    in one trading day; can hit ~1B on TSLA earnings days).
--  * put_call_ratio nullable (call_side=0 → undefined ratio; consumers
--    must NULL-check).
--  * flow_score nullable for the same reason.
--  * contract_count = how many distinct short-DTE contracts were
--    aggregated; useful for sanity-checking sparse-data days.
--  * Hypertable on trade_date with 90-day chunks — keeps the prune
--    pattern aligned with bars / chains / macro chunking.

BEGIN;

-- ── daily_options_flow ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_options_flow (
  underlying_ticker VARCHAR(10)    NOT NULL,
  trade_date        DATE           NOT NULL,
  call_volume       BIGINT         NOT NULL DEFAULT 0,
  put_volume        BIGINT         NOT NULL DEFAULT 0,
  call_oi           BIGINT         NOT NULL DEFAULT 0,
  put_oi            BIGINT         NOT NULL DEFAULT 0,
  put_call_ratio    DECIMAL(10,4),
  flow_score        DECIMAL(6,4),
  contract_count    INT            NOT NULL DEFAULT 0,
  fetched_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
  PRIMARY KEY (underlying_ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_options_flow_lookup
  ON daily_options_flow (underlying_ticker, trade_date);

-- TimescaleDB hypertable. trade_date is a DATE column, so chunk_time_interval
-- must be expressed as INTERVAL (timescale will treat it as days). 90 days
-- per chunk → ~4 chunks/year per symbol, well below the recommended
-- chunk-count ceiling. create_hypertable is idempotent (`if_not_exists`).
SELECT create_hypertable('daily_options_flow', 'trade_date',
  chunk_time_interval => INTERVAL '90 days',
  if_not_exists => TRUE);

-- ── daily_options_flow_misses ─────────────────────────────────────────
-- Range-shape miss markers. A row covers a contiguous range of trading
-- days where backfill (or the daily-refresh cron) found no chain data /
-- no aggregable contracts. Same shape as historical_options_chains_misses
-- so the shared RangeMarkerWriter helper applies unchanged.
CREATE TABLE IF NOT EXISTS daily_options_flow_misses (
  underlying_ticker VARCHAR(10)  NOT NULL,
  range_from        DATE         NOT NULL,
  range_to          DATE         NOT NULL,
  reason            TEXT,
  fetched_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (underlying_ticker, range_from, range_to)
);

CREATE INDEX IF NOT EXISTS idx_daily_options_flow_misses_lookup
  ON daily_options_flow_misses (underlying_ticker, range_from, range_to);

COMMIT;
