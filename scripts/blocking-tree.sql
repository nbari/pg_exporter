-- Who is blocking whom right now (lock-wait root cause).
-- Run when the Grafana "Locks & Blocking" panels go red (pg_blocked_sessions /
-- pg_longest_blocked_seconds). The metrics tell you THAT you're blocked and how
-- bad; this tells you WHO (PID) and WHAT (query) so you can act.
--
-- Act on the result:
--   * Terminate the culprit:  SELECT pg_terminate_backend(<blocking_pid>);
--   * Or fix the cause: long/idle-in-transaction sessions, slow UPDATEs from a
--     missing index, or DDL holding ACCESS EXCLUSIVE.
SELECT
    blocked.pid                                               AS blocked_pid,
    blocked.usename                                           AS blocked_user,
    blocked.datname                                           AS datname,
    round(extract(epoch FROM now() - blocked.query_start))::int AS wait_s,
    left(regexp_replace(blocked.query, '\s+', ' ', 'g'), 50)  AS blocked_query,
    blocking.pid                                              AS blocking_pid,
    blocking.usename                                          AS blocking_user,
    blocking.state                                            AS blocking_state,
    round(extract(epoch FROM now() - blocking.state_change))::int AS blocking_state_age_s,
    left(regexp_replace(blocking.query, '\s+', ' ', 'g'), 60) AS blocking_query
FROM pg_stat_activity blocked
JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS bp(pid) ON true
JOIN pg_stat_activity blocking ON blocking.pid = bp.pid
WHERE cardinality(pg_blocking_pids(blocked.pid)) > 0
ORDER BY wait_s DESC, blocked_pid
LIMIT 50;
