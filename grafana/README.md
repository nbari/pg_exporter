# PostgreSQL Monitoring Dashboard

A comprehensive Grafana dashboard designed to monitor PostgreSQL databases using metrics from `pg_exporter`.

## Quick Start

Simply import `dashboard.json` into your Grafana instance:

1. Open Grafana
2. Go to **Dashboards** → **Import**
3. Upload `dashboard.json` or paste its contents
4. Select your Prometheus datasource
5. Click **Import**

## Prometheus Scrape Interval

**Recommended scrape interval: 15-30 seconds**

```yaml
# Prometheus configuration
scrape_configs:
  - job_name: 'postgresql'
    scrape_interval: 30s  # Recommended default
    scrape_timeout: 10s   # Give enough time for queries
    static_configs:
      - targets: ['localhost:9187']
```

### Interval Guidelines:

- **30 seconds (Recommended)** - Best balance for production
  - Good data granularity without excessive load
  - PostgreSQL stats update every few seconds anyway
  - ~120 data points per hour
  - Lower Prometheus storage requirements

- **15 seconds** - For high-traffic or critical systems
  - Very fine-grained monitoring
  - Useful during incident investigation
  - Higher database and storage overhead

- **60 seconds** - For low-priority or development databases
  - Minimal overhead
  - Still good for trend analysis

**Why not 10 seconds?** The `statements` collector queries can be expensive on large databases, and PostgreSQL statistics don't update faster than a few seconds. Start with 30s and adjust based on your needs.

## Dashboard Sections

### 📊 Exporter Self-Monitoring (Collapsible)

**Enable with**: `--collector.exporter`

Monitor pg_exporter's own health and performance:

- **Exporter CPU Usage**: CPU % consumed by the exporter process
- **Exporter Memory Usage**: RSS and VSZ memory consumption
- **Total Metrics Exported**: Current metric cardinality (critical for Cortex/Mimir limits)
- **Collector Scrape Duration**: Per-collector performance with p50/p95/p99 percentiles
- **Collector Scrape Errors**: Failed collector scrapes (should always be zero)
- **Collector Health Status**: Success/failure status per collector
- **Last Scrape Time**: Detect stale collectors
- **Total Scrapes**: Verify exporter is active
- **Process Resources**: Threads and file descriptors

**Why monitor the exporter?**
- Detect resource leaks or high CPU usage
- Identify slow or failing collectors
- Track metric cardinality (prevent Cortex/Mimir rejections)
- Validate exporter health

### 🚨 Critical Alerts - Connection Pool & Performance
- **Connection Pool Utilization**: Gauge showing pool saturation (0-100%). Alert when >80%
- **Idle in Transaction**: Dangerous connections holding locks/snapshots
- **Connection Pool Status**: Max/Used/Available connections

### 🔌 Connection Analysis & Idle Age
- **Connections by Application**: Identify connection hogs (useful in K8s environments)
- **Idle Connection Age Buckets**: Detect connection leaks
  - `<1min` - Normal
  - `1-5min` - Acceptable
  - `5-15min` - Investigate
  - `15min-1h` - Likely leak
  - `>1h` - Definite leak!

### 🔍 Query Performance - pg_stat_statements

**Note**: Requires `pg_stat_statements` extension and the `statements` collector enabled.

**Existing Panels:**
- **Top 10 Queries by Total Execution Time**: Which queries consume the most DB time?
- **Top 10 Slowest Queries (Mean Time)**: Which queries are individually slow?
- **Query Call Rate**: Most frequently executed queries
- **Queries with Low Cache Hit Ratio (<90%)**: Queries doing excessive disk I/O
- **Queries Spilling to Disk**: Using temp files (may need more `work_mem`)
- **WAL Generation by Query**: Write-heavy queries (PostgreSQL 13+)

**Additional PromQL Queries** (copy to create custom panels):

```promql
# Detect N+1 Problems - Queries executed many times with few rows per call
topk(10, 
  postgres_pg_stat_statements_calls_total > 1000 
  and 
  (postgres_pg_stat_statements_rows_total / postgres_pg_stat_statements_calls_total) < 10
)

# Performance Regression Detection - Queries getting slower over time
increase(postgres_pg_stat_statements_mean_exec_time_seconds[1h]) > 0.5

# Queries Writing Temp Files - Need more work_mem
rate(postgres_pg_stat_statements_temp_blks_written_total[5m]) > 0

# High Variance Queries - Inconsistent performance (may have plan issues)
postgres_pg_stat_statements_stddev_exec_time_seconds 
/ 
postgres_pg_stat_statements_mean_exec_time_seconds > 0.5

# Disk-Heavy Queries - Cache misses indicate missing indexes or large scans
topk(10, 
  rate(postgres_pg_stat_statements_shared_blks_read_total[5m])
)
```

### 💾 Database Activity & I/O
- **Connections by Database & State**: Active/idle/idle_in_transaction breakdown
- **Waiting & Blocked Connections**: Lock contention indicators
- **Database Disk Reads**: I/O load per database
- **Transaction Throughput**: INSERT/UPDATE/DELETE operations per second

### 🔒 Locks & Blocking
- **Lock Count by Database & Mode**: Lock distribution across databases

### 🧹 Vacuum & Maintenance
- **Vacuum Progress**: Real-time vacuum operation tracking
- **Heap Blocks Vacuumed**: Vacuum work completed

### 📦 WAL
- **WAL Throughput**: Write-ahead log generation rate
- **WAL Record Activity**: WAL records and full-page images per second
- **WAL Buffer Pressure**: Rate of WAL buffer saturation events
- **FPI Ratio**: Full-page-image share of WAL records

### 🔄 Replication
- **Replication Lag**: Replay delay on standby nodes
- **Server Role**: Primary vs replica identification
- **Max Replication Lag Bytes**: Maximum WAL byte lag across replicas

### 📊 Table Statistics
- **Table DML Operations Rate**: Per-table INSERT/UPDATE/DELETE rates

### 💽 Database Size & Growth
- **Database Size**: Current size per database
- **Database Growth Rate**: Growth trend over time

## Template Variables

The dashboard includes three template variables for easy filtering:

- **Datasource**: Select your Prometheus datasource
- **Instance**: Filter by PostgreSQL instance (single selection)
- **Database**: Filter by database name (supports multi-select)

## Required Collectors

Different sections require different collectors to be enabled:

| Section | Collector | Default Enabled |
|---------|-----------|-----------------|
| Exporter Self-Monitoring | `exporter` | ❌ No (opt-in for self-monitoring) |
| Connection Pool & Alerts | `default`, `activity` | ✅ Yes |
| Query Performance | `statements` | ❌ No (requires `pg_stat_statements` extension) |
| Database Activity | `database`, `activity` | Partial |
| Locks | `locks` | ❌ No |
| Vacuum | `vacuum` | ✅ Yes |
| WAL | `default` | ✅ Yes |
| Replication | `default`, `replication` | Partial |
| Table Statistics | `stat` | ❌ No |
| Database Size | `database` | ❌ No |

### Enabling Collectors

**To use the full dashboard**, enable all required collectors:

#### Command-line flags:

```bash
pg_exporter \
    --collector.locks \
    --collector.database \
    --collector.stat \
    --collector.replication \
    --collector.index \
    --collector.statements \
    --collector.exporter  # Optional: for self-monitoring
```

#### Systemd service (recommended):

```ini
[Service]
Environment="POSTGRES_DSN=postgresql:///postgres?user=postgres_exporter"
ExecStart=/usr/local/bin/pg_exporter \
    --collector.locks \
    --collector.database \
    --collector.stat \
    --collector.replication \
    --collector.index \
    --collector.statements \
    --collector.exporter
```

#### Configuration file:

```toml
[collectors]
locks = true
database = true
stat = true
replication = true
index = true
statements = true
```

**Note**: The `statements` collector requires the `pg_stat_statements` extension (see setup below).

## pg_stat_statements Setup

For the Query Performance section, you need to install and configure the `pg_stat_statements` extension:

### 1. Install the Extension

```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

### 2. Configure PostgreSQL

Add to `postgresql.conf`:

```conf
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.track = all
pg_stat_statements.max = 10000
```

### 3. Restart PostgreSQL

```bash
sudo systemctl restart postgresql
```

### 4. Enable the Collector

```bash
pg_exporter --enable-collector=statements
```

## Key Metrics for On-Call & Troubleshooting

When responding to incidents, focus on these panels:

1. **Connection Pool Utilization** - Are we running out of connections?
2. **Idle in Transaction** - Are there stuck transactions holding locks?
3. **Top Queries by Total Execution Time** - What's killing the database right now?
4. **Waiting & Blocked Connections** - Is there lock contention?
5. **Transaction Throughput** - What's the write load?
6. **Idle Connection Age Buckets** - Are there connection leaks?

## Alert Recommendations

Set up Grafana alerts for:

- **Connection Pool Utilization > 80%** - Warning
- **Connection Pool Utilization > 90%** - Critical
- **Idle in Transaction > 0** for more than 1 minute - Warning
- **Idle in Transaction > 5** - Critical
- **Idle Age >1h** > 0 - Warning (connection leak)
- **Query Mean Exec Time > 1s** - Warning (for critical queries)

## Customization

Feel free to:
- Adjust the `topk()` limits (default: 10) to show more/fewer queries
- Modify time ranges and refresh intervals
- Add additional panels for your specific use cases
- Adjust threshold colors and values

## Contributing

Found a bug or have a suggestion? Please open an issue or PR in the `pg_exporter` repository.

## License

Same as `pg_exporter` - see main repository LICENSE file.

## Dashboard Validation

### Automated Testing for Dashboard Metrics

The dashboard includes automated validation to ensure all panels use metrics that pg_exporter actually exports. This prevents "No Data" errors from metric name mismatches.

#### Quick Start

```bash
# Validate dashboard
just validate-dashboard

# Or run directly
./scripts/validate-dashboard.sh
```

#### What It Checks

✅ All dashboard metrics exist in collector code  
✅ Handles histogram metrics (_bucket, _sum, _count)  
✅ JSON syntax is valid  
✅ Variable dependencies (job → instance → database)  
✅ All queries use job filter  

#### Example Output

```
🔍 Dashboard Validation
=======================

Step 1: Finding exported metrics...
  Found: 207 exported metrics

Step 2: Finding dashboard metrics...
  Found: 67 dashboard metrics

Step 3: Checking for invalid metrics...
  ✅ All dashboard metrics are valid!

Step 4: Validating JSON...
  ✅ JSON is valid

Step 5: Checking variables...
  ✅ Job variable exists
  ✅ Instance depends on job
  ✅ Database depends on job+instance
  ✅ 70/70 queries use job filter

=======================
✅ PASSED - Dashboard is valid!
```

#### When to Run

- Before committing dashboard changes
- After adding new metrics to collectors
- Before releasing new versions
- When troubleshooting "No Data" panels

#### Integration

The validation can be added to CI/CD:

```yaml
# GitHub Actions example
- name: Validate Dashboard
  run: ./scripts/validate-dashboard.sh
```

See `scripts/validate-dashboard.sh` for implementation details.
