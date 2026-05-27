-- 016 — intraday_atm_iv (Phase B.1 of live↔backtest IV parity).
--
-- Problem this solves: the existing daily_atm_iv table stores one row
-- per (underlying, trade_date), populated by the 08:00 ET cron with
-- yesterday's close-of-day average. Backtest's lagged-daily lookup
-- then uses N-1 day's value when evaluating bars on day N. This works
-- for slow vol regimes but fails on days where IV moves materially
-- intraday — 2026-05-26 in particular, where TSLA ATM IV crossed the
-- 0.45 "High" threshold during the session while yesterday's close
-- was 0.4174 ("Normal"). Live correctly classified High because it
-- polls Polygon's snapshot every ~5 minutes; backtest classified
-- Normal because its lookup returned yesterday's stale value.
--
-- Fix: capture the LIVE engine's ATM-IV reading at every refresh and
-- persist it here as a time-series. Backtest engines can then look
-- up the most recent intraday reading strictly before the bar being
-- evaluated (no lookahead — same invariant the daily lookup honors),
-- giving replay fidelity for days/minutes where intraday IV
-- diverges from the prior day's close.
--
-- Capture cadence is controlled by the live engine — currently
-- SignalSourcesService refreshes IV every ~5 minutes during RTH;
-- expect ~78 rows/symbol/day. Storage is cheap: at numeric(10,6) +
-- timestamp + smallint per row, ~50 bytes/row × 78 × 252 trading
-- days × 5 years × 1 symbol ≈ 5 MB. Per-symbol scaling is linear.
--
-- Read pattern: backtest looks up at-or-before (bar.Timestamp - 1min)
-- to honor the no-lookahead rule (a bar evaluated at its CLOSE can
-- consult IV state from before the close, not from the bar's close
-- itself — same convention IndicatorBundle uses for ToD classifier
-- since the live engine sees ATM IV updates lagging the bar tick).
--
-- TimescaleDB chunking: 7-day intervals (vs 90-day for daily_atm_iv).
-- The higher row cadence (~78/day vs 1/day) makes weekly chunks the
-- sweet spot for the typical 60-day backtest read window — ~9 chunks
-- spanned, fast index scan within each.

BEGIN;

-- ── intraday_atm_iv ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS intraday_atm_iv (
  underlying_ticker VARCHAR(10)   NOT NULL,
  captured_at       TIMESTAMPTZ   NOT NULL,
  atm_iv            NUMERIC(10,6) NOT NULL,
  contract_count    INT           NOT NULL,
  PRIMARY KEY (underlying_ticker, captured_at)
);

-- Lookup index optimized for the backtest read pattern: find the most
-- recent row at-or-before a timestamp for a given underlying.
CREATE INDEX IF NOT EXISTS idx_intraday_atm_iv_lookup
  ON intraday_atm_iv (underlying_ticker, captured_at DESC);

SELECT create_hypertable('intraday_atm_iv', 'captured_at',
  chunk_time_interval => INTERVAL '7 days',
  if_not_exists => TRUE);

COMMIT;
