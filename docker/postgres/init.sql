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
