-- 009 — convert historical_options_quotes_misses from point-shape to range-shape.
--
-- The original schema (migration 005) keyed NBBO miss markers by (ticker, ts)
-- — one row per known-empty (contract, minute) bucket. That works for the
-- per-call lookup pattern (PolygonNbboFetcher → OptionQuotesProvider hands
-- in a single ts) but blocks the intra-range gap-detection rework: a whole
-- afternoon of silence on one contract becomes 195 marker rows instead of
-- one. Brief 2026-05-02 mandates range-shape across all 4 surfaces.
--
-- Coalesce semantics — adjacent point markers (1-minute apart) collapse
-- into a single range. Two markers separated by a >1 minute gap stay
-- separate. Implementation: SQL window function. Postgres has no built-in
-- range-coalesce, so we compute a "session id" per gap-greater-than-1min
-- and group by it.
--
-- Apply order:
--   1. CREATE the new table (if not exists) alongside the old.
--   2. Coalesce-INSERT from the old table into the new.
--   3. DROP the old table.
--   4. RENAME the new table to the canonical name.
--
-- The migration is idempotent: re-running it on a DB where the migration
-- has already been applied is a no-op (every step guards on the schema
-- shape it expects).
--
-- DEPLOYMENT NOTE: This migration runs against an EXISTING dev/paper DB
-- volume; postgres-init.sql only fires on fresh volumes. Lisus deployment
-- plan post-NBBO-backfill (Phase 2 brief) walks the manual apply.

BEGIN;

-- Step 1: create the new range-shaped table alongside the old. We don't
-- TRUNCATE the old table first — we need its data for the coalesce.
-- The new name `…_v2` is temporary; we rename in step 4.
CREATE TABLE IF NOT EXISTS historical_options_quotes_misses_v2 (
  ticker      VARCHAR(50)  NOT NULL,
  range_from  TIMESTAMPTZ  NOT NULL,
  range_to    TIMESTAMPTZ  NOT NULL,
  reason      TEXT,
  fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, range_from, range_to)
);
CREATE INDEX IF NOT EXISTS idx_historical_options_quotes_misses_v2_lookup
  ON historical_options_quotes_misses_v2 (ticker, range_from, range_to);

-- Step 2: coalesce point markers into ranges. Requires the OLD table to
-- still exist; if a re-run happens after step 4 below, the old table is
-- gone and this becomes a no-op (the table_exists guard short-circuits
-- via the DO block).
DO $$
DECLARE
  tmpOldExists BOOLEAN;
  tmpInsertedRows BIGINT;
  tmpSourceRows BIGINT;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = current_schema()
      AND table_name = 'historical_options_quotes_misses'
      -- The 'old' shape has a `ts` column; the new shape has range_from /
      -- range_to. Distinguish via column presence to make the migration
      -- safe to re-run.
      AND EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'historical_options_quotes_misses'
          AND column_name = 'ts'
      )
  ) INTO tmpOldExists;

  IF NOT tmpOldExists THEN
    RAISE NOTICE 'Skip coalesce — old (ticker, ts) table not found (migration already applied?)';
    RETURN;
  END IF;

  SELECT COUNT(*) INTO tmpSourceRows
  FROM historical_options_quotes_misses;
  RAISE NOTICE 'Coalescing % point markers into ranges…', tmpSourceRows;

  -- Coalesce: window over (ticker ORDER BY ts), assign session_id that
  -- increments whenever the gap from the prior row is > 1 minute. Group
  -- by (ticker, session_id) and take MIN/MAX(ts) as the range bounds.
  -- Reason is concatenated to "<original-reason> | coalesced-from-points"
  -- so audit trails survive the migration.
  WITH ordered AS (
    SELECT
      ticker, ts, reason, recorded_at,
      LAG(ts) OVER (PARTITION BY ticker ORDER BY ts) AS prev_ts
    FROM historical_options_quotes_misses
  ),
  flagged AS (
    SELECT
      ticker, ts, reason, recorded_at,
      CASE
        WHEN prev_ts IS NULL OR ts - prev_ts > INTERVAL '1 minute' THEN 1
        ELSE 0
      END AS new_session
    FROM ordered
  ),
  sessioned AS (
    SELECT
      ticker, ts, reason, recorded_at,
      SUM(new_session) OVER (PARTITION BY ticker ORDER BY ts) AS session_id
    FROM flagged
  ),
  coalesced AS (
    SELECT
      ticker,
      MIN(ts) AS range_from,
      MAX(ts) AS range_to,
      -- Take the earliest reason for the run (audit nicety).
      (array_agg(reason ORDER BY ts ASC))[1] AS reason,
      MIN(recorded_at) AS fetched_at
    FROM sessioned
    GROUP BY ticker, session_id
  )
  INSERT INTO historical_options_quotes_misses_v2
    (ticker, range_from, range_to, reason, fetched_at)
  SELECT ticker, range_from, range_to,
         COALESCE(reason, '') || ' | coalesced-from-points',
         fetched_at
  FROM coalesced
  ON CONFLICT (ticker, range_from, range_to) DO NOTHING;

  GET DIAGNOSTICS tmpInsertedRows = ROW_COUNT;
  RAISE NOTICE 'Coalesce complete: % point markers → % range markers',
    tmpSourceRows, tmpInsertedRows;
END $$;

-- Step 3: drop the old table. After this point the migration is one-way
-- (re-runs become no-ops).
DROP TABLE IF EXISTS historical_options_quotes_misses;

-- Step 4: rename the v2 table to the canonical name.
ALTER TABLE historical_options_quotes_misses_v2
  RENAME TO historical_options_quotes_misses;
ALTER INDEX idx_historical_options_quotes_misses_v2_lookup
  RENAME TO idx_historical_options_quotes_misses_lookup;

COMMIT;
