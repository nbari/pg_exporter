-- Runs once on first postgres init (mounted into /docker-entrypoint-initdb.d/).
-- shared_preload_libraries = pg_stat_statements is set via the postgres command
-- flags in compose.yaml; this just creates the extension in the default database
-- so the collectors and tests find it immediately.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
