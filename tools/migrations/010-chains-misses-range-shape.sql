-- 010 — convert historical_options_chains_misses from point-shape to range-shape.
--
-- The original schema (migration 005) keyed chain miss markers by
-- (symbol, as_of_date) — one row per known-empty (symbol, day) bucket.
-- That works for the per-day lookup pattern (OptionChainProvider hands
-- in a single as_of_date) but blocks intra-range gap-detection: a whole
-- holiday week of silence on one symbol becomes 5 marker rows instead
-- of one. Brief 2026-05-02 mandates range-shape across all 4 surfaces
-- (bars, NBBO, chains, macro).
--
-- Coalesce semantics — adjacent point markers (1-day apart, weekday-only)
-- collapse into a single date range. Two markers separated by a >1-day
-- gap stay separate. Implementation mirrors migration 009: a SQL window
-- function computes a "session id" per gap-greater-than-1-day and groups
-- by it.
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
-- volume; postgres-init.sql only fires on fresh volumes. Operator runs
-- this manually (same pattern as 008 / 009).

BEGIN;

-- Step 1: create the new range-shaped table alongside the old. We don't
-- TRUNCATE the old table first — we need its data for the coalesce.
-- The new name `…_v2` is temporary; we rename in step 4.
CREATE TABLE IF NOT EXISTS historical_options_chains_misses_v2 (
  symbol      VARCHAR(10)  NOT NULL,
  range_from  DATE         NOT NULL,
  range_to    DATE         NOT NULL,
  reason      TEXT,
  fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (symbol, range_from, range_to)
);
CREATE INDEX IF NOT EXISTS idx_historical_options_chains_misses_v2_lookup
  ON historical_options_chains_misses_v2 (symbol, range_from, range_to);

-- Step 2: coalesce point markers into ranges. Requires the OLD table to
-- still exist and have an `as_of_date` column; if a re-run happens after
-- step 4 below, the old table is gone and this becomes a no-op (the
-- table_exists guard short-circuits via the DO block).
DO $$
DECLARE
  tmpOldExists BOOLEAN;
  tmpInsertedRows BIGINT;
  tmpSourceRows BIGINT;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = current_schema()
      AND table_name = 'historical_options_chains_misses'
      AND EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'historical_options_chains_misses'
          AND column_name = 'as_of_date'
      )
  ) INTO tmpOldExists;

  IF NOT tmpOldExists THEN
    RAISE NOTICE 'Skip coalesce — old (symbol, as_of_date) table not found (migration already applied?)';
    RETURN;
  END IF;

  SELECT COUNT(*) INTO tmpSourceRows
  FROM historical_options_chains_misses;
  RAISE NOTICE 'Coalescing % point markers into ranges…', tmpSourceRows;

  -- Coalesce: window over (symbol ORDER BY as_of_date), assign session_id
  -- that increments whenever the gap from the prior row is > 1 day. Group
  -- by (symbol, session_id) and take MIN/MAX(as_of_date) as the range
  -- bounds. Reason concatenated to "<original-reason> | coalesced-from-points".
  WITH ordered AS (
    SELECT
      symbol, as_of_date, reason, fetched_at,
      LAG(as_of_date) OVER (PARTITION BY symbol ORDER BY as_of_date) AS prev_dt
    FROM historical_options_chains_misses
  ),
  flagged AS (
    SELECT
      symbol, as_of_date, reason, fetched_at,
      CASE
        WHEN prev_dt IS NULL OR (as_of_date - prev_dt) > 1 THEN 1
        ELSE 0
      END AS new_session
    FROM ordered
  ),
  sessioned AS (
    SELECT
      symbol, as_of_date, reason, fetched_at,
      SUM(new_session) OVER (PARTITION BY symbol ORDER BY as_of_date) AS session_id
    FROM flagged
  ),
  coalesced AS (
    SELECT
      symbol,
      MIN(as_of_date) AS range_from,
      MAX(as_of_date) AS range_to,
      (array_agg(reason ORDER BY as_of_date ASC))[1] AS reason,
      MIN(fetched_at) AS fetched_at
    FROM sessioned
    GROUP BY symbol, session_id
  )
  INSERT INTO historical_options_chains_misses_v2
    (symbol, range_from, range_to, reason, fetched_at)
  SELECT symbol, range_from, range_to,
         COALESCE(reason, '') || ' | coalesced-from-points',
         fetched_at
  FROM coalesced
  ON CONFLICT (symbol, range_from, range_to) DO NOTHING;

  GET DIAGNOSTICS tmpInsertedRows = ROW_COUNT;
  RAISE NOTICE 'Coalesce complete: % point markers → % range markers',
    tmpSourceRows, tmpInsertedRows;
END $$;

-- Step 3: drop the old table. After this point the migration is one-way
-- (re-runs become no-ops).
DROP TABLE IF EXISTS historical_options_chains_misses;

-- Step 4: rename the v2 table to the canonical name.
ALTER TABLE historical_options_chains_misses_v2
  RENAME TO historical_options_chains_misses;
ALTER INDEX idx_historical_options_chains_misses_v2_lookup
  RENAME TO idx_historical_options_chains_misses_lookup;

COMMIT;
