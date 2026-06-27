-- Connection breakdown by database / user / application / state. Pairs with the
-- Grafana "Connection Pool Utilization" and "Connections by Application" panels.
--
-- Without a pooler (pgbouncer), apps open many direct connections. This shows WHO is
-- holding them: which application/user has the most, and how many are stuck in a
-- given state (watch for large `idle in transaction` counts -- they hold locks).
-- `max_state_age_s` is how long the oldest connection has sat in that state.
--
-- Act: cap app pool sizes / add pgbouncer; set idle_in_transaction_session_timeout;
-- terminate leaks with SELECT pg_terminate_backend(<pid>) (find pids via just long-running).
SELECT
    COALESCE(datname, '-')                                AS datname,
    usename                                               AS usename,
    COALESCE(NULLIF(application_name, ''), '-')           AS application_name,
    state,
    count(*)                                              AS conns,
    max(round(extract(epoch FROM now() - state_change))::int) AS max_state_age_s
FROM pg_stat_activity
WHERE backend_type = 'client backend'
GROUP BY datname, usename, application_name, state
ORDER BY conns DESC
LIMIT 50;
