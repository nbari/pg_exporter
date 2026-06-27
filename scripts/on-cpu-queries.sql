-- What is running on CPU right now (CPU-saturation root cause).
-- Run when the Grafana "CPU Pressure" panels are high (pg_stat_activity_on_cpu_backends
-- near/above the vCPU count). The metric tells you HOW MANY backends are burning CPU;
-- this shows WHICH ones (PID + query + how long they've been running).
--
-- "On CPU" = active client backends that are NOT waiting on any event
-- (state = 'active' AND wait_event IS NULL) -- i.e. actually consuming a core now.
--
-- Act on the result:
--   * Cancel a runaway query:     SELECT pg_cancel_backend(<pid>);   (gentle)
--   * Force-terminate if needed:  SELECT pg_terminate_backend(<pid>);
--   * Fix the cause: a missing index causing seq scans (see the Table Statistics
--     row), an unbounded query, or too much concurrency without a pooler.
-- For cumulative CPU hogs across time, also check pg_stat_statements ordered by
-- total_exec_time (see scripts and the Query Performance dashboard row).
SELECT
    pid,
    usename                                                   AS usename,
    datname                                                   AS datname,
    round(extract(epoch FROM now() - query_start)::numeric, 1) AS running_s,
    backend_type,
    left(regexp_replace(query, '\s+', ' ', 'g'), 80)          AS query
FROM pg_stat_activity
WHERE state = 'active'
  AND wait_event IS NULL
  AND backend_type = 'client backend'
  AND pid <> pg_backend_pid()
ORDER BY query_start
LIMIT 50;
