-- Long-running and idle-in-transaction sessions (the #1 hidden cause of blocking
-- and connection exhaustion). Pairs with the Grafana "Long-Running Query Age" and
-- "Idle in Transaction (Dangerous!)" panels.
--
-- idle-in-transaction sessions hold locks/snapshots while doing nothing -> they
-- block others (see `just blocking`) and bloat tables (vacuum can't clean rows they
-- can still see). Long active queries burn CPU/IO.
--
-- Act:
--   * Cancel a query:  SELECT pg_cancel_backend(<pid>);
--   * Kill a stuck/idle-in-tx session:  SELECT pg_terminate_backend(<pid>);
--   * Fix the app: close transactions promptly; set idle_in_transaction_session_timeout.
SELECT
    pid,
    usename                                                            AS usename,
    datname                                                            AS datname,
    state,
    round(extract(epoch FROM now() - COALESCE(query_start, xact_start))::numeric, 1) AS age_s,
    wait_event_type,
    wait_event,
    left(regexp_replace(left(query, 256), '\s+', ' ', 'g'), 70)        AS query
FROM pg_stat_activity
WHERE backend_type = 'client backend'
  AND pid <> pg_backend_pid()
  AND (state = 'active' OR state LIKE 'idle in transaction%')
  AND now() - COALESCE(query_start, xact_start) > interval '5 seconds'
ORDER BY age_s DESC
LIMIT 50;
