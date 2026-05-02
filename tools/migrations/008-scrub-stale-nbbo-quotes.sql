-- 008 — scrub stale NBBO quotes from historical_options_quotes.
--
-- Companion to fix/nbbo-freshness-gate (PolygonNbboFetcher.cs +300s gate).
-- Pre-fix, the fetcher persisted any quote that Polygon returned for a
-- timestamp.lte=<bucket>&order=desc&limit=1 lookup, INCLUDING quotes
-- whose sip_timestamp was hours or days older than the bucket — e.g.
-- prior-session close NBBOs stamped under today's open bucket.
--
-- Surfaced 2026-05-02 by Lisus's screenshot:
--   ticker=O:SPY240103P00450000 ts=2024-01-02 14:30:00+00 (Tue 09:30 ET open)
--   as_of_ts=2023-12-29 21:14:19+00 (Fri 16:14 ET prior-day close)
-- That row is 3.7 days stale. With the freshness gate now in place
-- (max age = 300s), no future row will land like this. This migration
-- removes the historical bad rows already in the cache.
--
-- Survey before running this script (mbd-history-postgres,
-- 2026-05-02 captured by Lisus dispatch):
--
--   total                    1,444,366
--   stale_rows (>60s gap)       25,274
--     - lt_5min                  5,326   ← borderline; we keep these
--                                          since the read-side already
--                                          tolerates ≤300s.
--     - lt_1hour                 6,788   ← delete
--     - lt_1day                 10,462   ← delete
--     - lt_7day                  2,698   ← delete
--   cross-day rows              12,816   (subset of the above)
--
-- Threshold matches the fetcher's DefaultMaxQuoteAgeSeconds (300s):
-- delete any row where (ts - as_of_ts) > 300s. This is consistent with
-- "what the fetcher would have rejected if the gate had been in place."
--
-- DO NOT RUN WITHOUT LISUS GO/NO-GO. Wrap in a transaction so a typo
-- doesn't blast good data; surface the row count before COMMIT.
--
-- Estimated affected: ~19,948 rows (~1.4% of total). Read-side queries
-- for these (ticker, ts) pairs will then return the at-or-before fuzzy
-- match (next-fresher cached row) or null — same behavior as if the
-- bucket had never been written.

BEGIN;

-- Preview row count (run this first, surface to Lisus, then COMMIT).
SELECT COUNT(*) AS rows_to_delete,
       MIN(ts)  AS earliest_bucket,
       MAX(ts)  AS latest_bucket
FROM historical_options_quotes
WHERE as_of_ts IS NOT NULL
  AND as_of_ts < ts - INTERVAL '300 seconds';

-- Per-underlying breakdown (sanity check before commit).
SELECT SUBSTRING(ticker FROM 'O:([A-Z]+)') AS underlying,
       COUNT(*) AS n
FROM historical_options_quotes
WHERE as_of_ts IS NOT NULL
  AND as_of_ts < ts - INTERVAL '300 seconds'
GROUP BY 1
ORDER BY 2 DESC;

-- The actual delete. Commented out — un-comment after Lisus confirms.
--
-- DELETE FROM historical_options_quotes
-- WHERE as_of_ts IS NOT NULL
--   AND as_of_ts < ts - INTERVAL '300 seconds';

ROLLBACK;
-- COMMIT;
