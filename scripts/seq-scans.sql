-- Tables most likely missing an index: high sequential-scan activity, large size,
-- low index usage. Pairs with the Grafana "Table Statistics" / missing-index panels.
--
-- Read it as: tables high in `seq_tup_read` with a low `idx_use_pct` and a large
-- `table_size` are the strongest index candidates. `avg_rows_per_scan` shows how many
-- rows each sequential scan reads (high = scanning a lot repeatedly).
--
-- Act: find the actual query in pg_stat_statements (`just top-queries`), confirm with
-- EXPLAIN (ANALYZE, BUFFERS), then CREATE INDEX matching its predicates/joins/order.
-- Beware write-heavy tables: each index adds insert/update/delete + WAL + vacuum cost.
SELECT
    schemaname,
    relname,
    seq_scan,
    seq_tup_read,
    CASE WHEN seq_scan > 0 THEN (seq_tup_read / seq_scan) ELSE 0 END    AS avg_rows_per_scan,
    idx_scan,
    CASE WHEN (seq_scan + idx_scan) > 0
         THEN round(100.0 * idx_scan / (seq_scan + idx_scan), 1)
         ELSE NULL END                                                  AS idx_use_pct,
    n_live_tup,
    pg_size_pretty(pg_relation_size(relid))                            AS table_size
FROM pg_stat_user_tables
WHERE seq_scan > 0
ORDER BY seq_tup_read DESC
LIMIT 25;
