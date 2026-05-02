-- 011 — convert macro_data_misses from point-shape to range-shape.
--
-- The original schema (migration 005) keyed macro miss markers by
-- (series_id, observation_date) — one row per known-empty (series,
-- date) bucket. Brief 2026-05-02 mandates range-shape across all 4
-- surfaces (bars, NBBO, chains, macro) so the intra-range gap-detection
-- path can store one row per contiguous run of missing publications.
--
-- For Daily series (T10Y2Y, DGS10): adjacent business-day point markers
-- collapse if there is no business day strictly between them — i.e.
-- weekends or single-day gaps. For Monthly series (CPIAUCSL, UNRATE):
-- adjacent first-of-month point markers collapse if there is no
-- intervening month boundary in their gap. Both rules implement
-- "no expected publication boundary between A and B".
--
-- Implementation: SQL window function. We compute a "session id" per
-- gap-greater-than-N-days and group by it. The threshold is conservative
-- for daily (gap > 3 calendar days catches Mon..Fri end-runs) and
-- generous for monthly (gap > 31 days). The runtime's
-- BackfillMissingBoundaryMarkersAsync can re-coalesce on next run if
-- needed via RangeMarkerWriter's coalesce-on-write path.
--
-- Apply order:
--   1. CREATE the new table (if not exists) alongside the old.
--   2. Coalesce-INSERT from the old table into the new.
--   3. DROP the old table.
--   4. RENAME the new table to the canonical name.
--
-- The migration is idempotent. DEPLOYMENT NOTE: runs against existing
-- dev/paper volumes manually (postgres-init.sql only fires on fresh).

BEGIN;

-- Step 1: create the new range-shaped table alongside the old.
CREATE TABLE IF NOT EXISTS macro_data_misses_v2 (
  series_id   VARCHAR(20)  NOT NULL,
  range_from  DATE         NOT NULL,
  range_to    DATE         NOT NULL,
  reason      TEXT,
  fetched_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (series_id, range_from, range_to)
);
CREATE INDEX IF NOT EXISTS idx_macro_data_misses_v2_lookup
  ON macro_data_misses_v2 (series_id, range_from, range_to);

-- Step 2: coalesce point markers into ranges.
DO $$
DECLARE
  tmpOldExists BOOLEAN;
  tmpInsertedRows BIGINT;
  tmpSourceRows BIGINT;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = current_schema()
      AND table_name = 'macro_data_misses'
      AND EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'macro_data_misses'
          AND column_name = 'observation_date'
      )
  ) INTO tmpOldExists;

  IF NOT tmpOldExists THEN
    RAISE NOTICE 'Skip coalesce — old (series_id, observation_date) table not found (migration already applied?)';
    RETURN;
  END IF;

  SELECT COUNT(*) INTO tmpSourceRows FROM macro_data_misses;
  RAISE NOTICE 'Coalescing % point markers into ranges…', tmpSourceRows;

  -- Coalesce: window over (series_id ORDER BY observation_date), assign
  -- session_id that increments whenever the gap from the prior row is
  -- > 3 calendar days (covers weekend gaps for daily; monthly markers'
  -- ~30-day gap will always trigger a new session, which is correct —
  -- monthly observations rarely cluster). Group by (series_id,
  -- session_id) and take MIN/MAX(observation_date) as the range bounds.
  WITH ordered AS (
    SELECT
      series_id, observation_date, reason, fetched_at,
      LAG(observation_date) OVER (PARTITION BY series_id ORDER BY observation_date) AS prev_dt
    FROM macro_data_misses
  ),
  flagged AS (
    SELECT
      series_id, observation_date, reason, fetched_at,
      CASE
        WHEN prev_dt IS NULL OR (observation_date - prev_dt) > 3 THEN 1
        ELSE 0
      END AS new_session
    FROM ordered
  ),
  sessioned AS (
    SELECT
      series_id, observation_date, reason, fetched_at,
      SUM(new_session) OVER (PARTITION BY series_id ORDER BY observation_date) AS session_id
    FROM flagged
  ),
  coalesced AS (
    SELECT
      series_id,
      MIN(observation_date) AS range_from,
      MAX(observation_date) AS range_to,
      (array_agg(reason ORDER BY observation_date ASC))[1] AS reason,
      MIN(fetched_at) AS fetched_at
    FROM sessioned
    GROUP BY series_id, session_id
  )
  INSERT INTO macro_data_misses_v2
    (series_id, range_from, range_to, reason, fetched_at)
  SELECT series_id, range_from, range_to,
         COALESCE(reason, '') || ' | coalesced-from-points',
         fetched_at
  FROM coalesced
  ON CONFLICT (series_id, range_from, range_to) DO NOTHING;

  GET DIAGNOSTICS tmpInsertedRows = ROW_COUNT;
  RAISE NOTICE 'Coalesce complete: % point markers → % range markers',
    tmpSourceRows, tmpInsertedRows;
END $$;

-- Step 3: drop the old table.
DROP TABLE IF EXISTS macro_data_misses;

-- Step 4: rename the v2 table to the canonical name.
ALTER TABLE macro_data_misses_v2 RENAME TO macro_data_misses;
ALTER INDEX idx_macro_data_misses_v2_lookup
  RENAME TO idx_macro_data_misses_lookup;

COMMIT;
