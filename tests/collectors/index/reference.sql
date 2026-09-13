-- Frozen 0.20.0 (5e86f48) reference. Keep independent of the production query.
WITH original_stats AS (
    SELECT
        current_database() AS datname,
        COALESCE(SUM(s.idx_scan), 0)::bigint AS total_scans,
        COALESCE(SUM(s.idx_tup_read), 0)::bigint AS total_tup_read,
        COALESCE(SUM(s.idx_tup_fetch), 0)::bigint AS total_tup_fetch,
        COALESCE(SUM(pg_relation_size(s.indexrelid)), 0)::bigint AS total_size_bytes,
        COALESCE(SUM(i.indisvalid::int), 0)::bigint AS valid_count,
        COALESCE(SUM(io.idx_blks_read), 0)::bigint AS total_idx_blks_read,
        COALESCE(SUM(io.idx_blks_hit), 0)::bigint AS total_idx_blks_hit
    FROM pg_stat_user_indexes s
    JOIN pg_index i ON s.indexrelid = i.indexrelid
    LEFT JOIN pg_statio_user_indexes io ON s.indexrelid = io.indexrelid
    WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema')
    ), original_unused AS (
    SELECT
        current_database() AS datname,
        (
            SELECT COUNT(*)::bigint
            FROM pg_stat_user_indexes s
            JOIN pg_index i ON s.indexrelid = i.indexrelid
            WHERE s.idx_scan = 0
              AND NOT i.indisprimary
              AND NOT i.indisunique
              AND s.schemaname NOT IN ('pg_catalog', 'information_schema')
        ) AS unused_count,
        (
            SELECT COALESCE(SUM(pg_relation_size(s.indexrelid)), 0)::bigint
            FROM pg_stat_user_indexes s
            JOIN pg_index i ON s.indexrelid = i.indexrelid
            WHERE s.idx_scan = 0
              AND NOT i.indisprimary
              AND NOT i.indisunique
              AND s.schemaname NOT IN ('pg_catalog', 'information_schema')
        ) AS unused_size_bytes,
        (
            SELECT COUNT(*)::bigint
            FROM pg_index i
            JOIN pg_class c ON i.indexrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE NOT i.indisvalid
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ) AS invalid_count
    )
SELECT s.*, u.unused_count, u.unused_size_bytes, u.invalid_count
FROM original_stats s JOIN original_unused u USING (datname)
