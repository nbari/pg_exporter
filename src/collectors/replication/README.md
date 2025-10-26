# Replication Collector

This collector tracks PostgreSQL replication metrics, compatible with [postgres_exporter](https://github.com/prometheus-community/postgres_exporter).

## Overview

The replication collector provides three sub-collectors:

1. **Replica Status** - Monitors standby/replica server metrics
2. **pg_stat_replication** - Tracks replication slots from primary perspective
3. **pg_replication_slots** - Monitors physical and logical replication slots

## Metrics

### Replica Status (standby servers)

Compatible with `postgres_exporter` pg_replication namespace:

- `pg_replication_lag_seconds` - Replication lag behind primary in seconds
- `pg_replication_is_replica` - Whether server is replica (1) or primary (0)
- `pg_replication_last_replay_seconds` - Age of last transaction replay in seconds

### pg_stat_replication (primary servers)

Labels: `application_name`, `client_addr`, `state`

- `pg_stat_replication_pg_current_wal_lsn_bytes` - Current WAL LSN on primary in bytes
- `pg_stat_replication_pg_wal_lsn_diff` - Lag in bytes between primary WAL LSN and replica replay LSN
- `pg_stat_replication_reply_time` - Time since last reply from replica in seconds
- `pg_stat_replication_slots` - Number of replication slots by application and state

### pg_replication_slots

Labels: `slot_name`, `slot_type`, `database`

- `pg_replication_slots_pg_wal_lsn_diff` - Replication slot lag in bytes
- `pg_replication_slots_active` - Whether slot is active (1) or inactive (0)

## Usage

Enable the replication collector:

```bash
pg_exporter --dsn postgresql://user@localhost/postgres --collector.replication
```

Or disable it if needed:

```bash
pg_exporter --dsn postgresql://user@localhost/postgres --no-collector.replication
```

## Example Queries

### Monitor replication lag on standby

```promql
pg_replication_lag_seconds > 10
```

### Alert on high WAL lag between primary and replicas

```promql
pg_stat_replication_pg_wal_lsn_diff > 1073741824  # 1GB in bytes
```

### Check for inactive replication slots

```promql
pg_replication_slots_active == 0
```

### Monitor replication slot lag

```promql
pg_replication_slots_pg_wal_lsn_diff > 10737418240  # 10GB in bytes
```

## PostgreSQL Version Compatibility

- Requires PostgreSQL 10.0+
- Uses modern WAL functions (`pg_current_wal_lsn`, `pg_wal_lsn_diff`)
- Works on both primary and standby servers
- Gracefully handles absence of replicas or replication slots

## Implementation Notes

- Compatible with postgres_exporter metric names and labels
- All metrics handle NULL values gracefully
- Concurrent collection safe
- Works whether server is primary or standby
- No metrics exported when no replicas/slots exist (avoids cardinality issues)
