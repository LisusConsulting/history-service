-- 013 — historical_options_snapshots + historical_options_snapshots_misses.
--
-- Wave A / PR 1 of the ATM-IV full historical coverage plan
-- (docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md). This
-- table is the unified storage for option snapshots across two sources:
--
--   * source='polygon_live' rows — captured by the live-capture cron
--     (PR 4) every 5 minutes during RTH from Polygon's
--     /v3/snapshot/options/{underlying}; also bootstrapped from MBD's
--     existing dev `options_snapshots` table (Wave C / PR 9).
--
--   * source='computed_bs' rows — written by the BS-snapshot seeder
--     (PR 3) for historical dates that pre-date the live-capture
--     period. The seeder reads NBBO + bars + DGS3MO, computes IV via
--     Black-Scholes (Newton-Raphson → Brent), and persists with NULL
--     IV/greeks on solver failure.
--
-- Aggregation downstream: PR 5's GetDailyAtmIv RPC reads
-- `daily_atm_iv` (migration 014) which is populated by the daily
-- 08:00 ET cron + seeder backfill aggregating from this table.
--
-- Key design points (locked in plan, 2026-05-03):
--   * Mid-price for IV solve = (bid + ask) / 2; the seeder skips rows
--     where bid > ask, bid <= 0, or ask <= 0 and writes NULL IV/greeks
--     for those.
--   * `source` is the provenance tag — required NOT NULL with a CHECK
--     constraint to prevent typos. Splitting the table by source was
--     considered + rejected; consumers want one table to query.
--   * Hypertable on snapshot_date with 7-day chunks. RTH-cadence
--     polygon_live rows produce ~78 rows/(contract,day); 7-day chunks
--     keep per-chunk size in the ~MB range for prune-friendliness.
--   * fetched_at not stored — `snapshot_date` already carries
--     captured-at semantics; an audit field was deemed redundant.
--     (Distinction from migration 012 which has both: there
--     `trade_date` is a calendar date with no timestamp, so a
--     fetched_at is meaningful.)

BEGIN;

-- ── historical_options_snapshots ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS historical_options_snapshots (
  ticker             VARCHAR(50)   NOT NULL,
  snapshot_date      TIMESTAMPTZ   NOT NULL,
  bid_price          NUMERIC(18,4),
  ask_price          NUMERIC(18,4),
  volume             BIGINT,
  open_interest      BIGINT,
  implied_volatility NUMERIC(10,6),
  delta              NUMERIC(10,6),
  gamma              NUMERIC(10,6),
  theta              NUMERIC(10,6),
  vega               NUMERIC(10,6),
  underlying_price   NUMERIC(18,4),
  source             VARCHAR(16)   NOT NULL
    CHECK (source IN ('polygon_live', 'computed_bs')),
  PRIMARY KEY (ticker, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_historical_options_snapshots_lookup
  ON historical_options_snapshots (ticker, snapshot_date);

-- 7-day chunks. polygon_live cadence (5-min × RTH × ~100 ATM-band
-- contracts) produces ~78,000 rows/contract/week; 7-day chunks keep
-- per-chunk size in the low-MB range for cheap prunes.
SELECT create_hypertable('historical_options_snapshots', 'snapshot_date',
  chunk_time_interval => INTERVAL '7 days',
  if_not_exists => TRUE);

-- ── historical_options_snapshots_misses ─────────────────────────────────
-- Range-shape miss markers, identical shape to the chains/macro/flow
-- miss tables. Keyed by (ticker, range_from, range_to) where ticker is
-- the option contract symbol (O:TSLA241220C00250000) and range_from/to
-- are TIMESTAMPTZ to align with the parent table's snapshot_date type.
-- Unlike daily_options_flow_misses which uses DATE bounds (per-day
-- granularity), this table uses TIMESTAMPTZ because misses on the live
-- snapshot surface are minute-granular (a 5-min cadence tick that
-- returns no row marks that single instant, not a whole day).
CREATE TABLE IF NOT EXISTS historical_options_snapshots_misses (
  ticker      VARCHAR(50)  NOT NULL,
  range_from  TIMESTAMPTZ  NOT NULL,
  range_to    TIMESTAMPTZ  NOT NULL,
  reason      TEXT,
  fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, range_from, range_to)
);

CREATE INDEX IF NOT EXISTS idx_historical_options_snapshots_misses_lookup
  ON historical_options_snapshots_misses (ticker, range_from, range_to);

COMMIT;
