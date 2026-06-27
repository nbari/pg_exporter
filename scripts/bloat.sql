-- Tables with the most dead tuples / highest dead-tuple ratio: candidates for
-- aggressive autovacuum tuning, VACUUM, or pg_repack / VACUUM FULL. Pairs with the
-- Grafana "Vacuum & Bloat Pressure" row (dead tuples, bloat ratio, repack candidates).
--
-- High `dead_pct` on a large `table_size` with an old `last_autovacuum` means dead
-- rows are accumulating faster than autovacuum reclaims them -> bloat, slower scans.
--
-- Act:
--   * Reclaim for reuse (no lock):  VACUUM (VERBOSE, ANALYZE) <table>;
--   * Compact and return space:     pg_repack (low downtime) or VACUUM FULL (takes
--                                   ACCESS EXCLUSIVE -> maintenance window).
--   * Tune per-table autovacuum if it recurs (scale factor / cost limit).
SELECT
    schemaname,
    relname,
    n_dead_tup,
    n_live_tup,
    CASE WHEN (n_live_tup + n_dead_tup) > 0
         THEN round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 1)
         ELSE 0 END                                       AS dead_pct,
    pg_size_pretty(pg_relation_size(relid))               AS table_size,
    last_autovacuum,
    last_vacuum
FROM pg_stat_user_tables
WHERE n_dead_tup > 0
ORDER BY n_dead_tup DESC
LIMIT 25;
