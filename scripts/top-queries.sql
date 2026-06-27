-- Top queries by cumulative execution time (the heaviest CPU/latency consumers since
-- stats were last reset). Pairs with the Grafana "Query Performance" row.
--
-- Requires the pg_stat_statements extension (CREATE EXTENSION pg_stat_statements;
-- and shared_preload_libraries). total_exec_time is PostgreSQL 13+.
--
-- Read it as: high `total_s` = where the database spends the most time overall.
-- High `mean_ms` with low `calls` = a slow query; low `mean_ms` with huge `calls` =
-- a fast query run too often. Low `hit_pct` = I/O-bound; high = CPU-bound.
--
-- Act: optimize the plan (often a missing index -> `just seq-scans`), reduce call
-- volume/N+1 patterns, or rewrite. Use EXPLAIN (ANALYZE, BUFFERS) on the real query.
SELECT
    round((total_exec_time / 1000)::numeric, 1)                        AS total_s,
    calls,
    round(mean_exec_time::numeric, 2)                                  AS mean_ms,
    round(100.0 * shared_blks_hit / NULLIF(shared_blks_hit + shared_blks_read, 0), 1) AS hit_pct,
    rows,
    left(regexp_replace(query, '\s+', ' ', 'g'), 70)                   AS query
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;
