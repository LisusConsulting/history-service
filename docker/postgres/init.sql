-- docker/postgres/init.sql — first-boot bootstrap for mbd-history-postgres.
--
-- The timescaledb image runs every .sql in /docker-entrypoint-initdb.d on
-- a fresh data volume. We mount this single file there and concatenate the
-- numbered migrations in order so the DB comes up with the full Phase 1
-- schema in one shot. Idempotent (every CREATE is IF NOT EXISTS) so an
-- accidental re-run on a non-empty volume is harmless.
--
-- Migrations live in /tools/migrations/ in the repo and are mounted into
-- the container alongside this file (see docker-compose.yml volumes).

\i /docker-entrypoint-initdb.d/migrations/001-create-historical-bars.sql
\i /docker-entrypoint-initdb.d/migrations/002-create-historical-options-quotes.sql
\i /docker-entrypoint-initdb.d/migrations/003-create-historical-options-contracts.sql
\i /docker-entrypoint-initdb.d/migrations/004-create-macro-data.sql
\i /docker-entrypoint-initdb.d/migrations/005-create-miss-markers.sql
\i /docker-entrypoint-initdb.d/migrations/006-create-cache-stats.sql
-- 009 is a no-op on fresh volumes (the v1 schema with `ts` column is
-- never created here), but we list it so the file order stays in sync
-- with the migrations directory and the Phase 2 deployment plan.
\i /docker-entrypoint-initdb.d/migrations/009-nbbo-misses-range-shape.sql
-- 010 is a no-op on fresh volumes (the v1 schema with `as_of_date` column
-- is never created here), but we list it so the file order stays in sync
-- with the migrations directory and the deployment plan.
\i /docker-entrypoint-initdb.d/migrations/010-chains-misses-range-shape.sql
-- 011 is a no-op on fresh volumes (the v1 schema with `observation_date`
-- column is never created here), but we list it so the file order stays
-- in sync with the migrations directory and the deployment plan.
\i /docker-entrypoint-initdb.d/migrations/011-macro-misses-range-shape.sql
-- 012 creates daily_options_flow + daily_options_flow_misses on fresh
-- volumes so the backtest reader has the schema available immediately;
-- on existing volumes the operator runs this migration once to create
-- the new tables (idempotent — every CREATE is IF NOT EXISTS).
\i /docker-entrypoint-initdb.d/migrations/012-daily-options-flow.sql
-- 013 + 014 — Wave A / PR 1 of the ATM-IV full historical coverage plan
-- (docs/research/atm-iv-full-historical-coverage-plan-2026-05-03.md):
-- creates historical_options_snapshots + daily_atm_iv (+ both *_misses
-- tables). Idempotent so an accidental re-run on a non-empty volume is
-- harmless. On existing volumes the operator runs these migrations
-- manually before the live-capture cron (PR 4) and seeder backfill
-- (PR 3) deploy.
\i /docker-entrypoint-initdb.d/migrations/013-historical-options-snapshots.sql
\i /docker-entrypoint-initdb.d/migrations/014-daily-atm-iv.sql
-- 015 — Phase 4 of mbd-data-provider centralization (2026-05-09).
-- Creates option_contract_snapshots hypertable that data-provider rolls
-- over here at end-of-day. Polygon doesn't time-travel for these
-- intraday snapshots, so this is the only persistent record.
\i /docker-entrypoint-initdb.d/migrations/015-create-option-contract-snapshots.sql
