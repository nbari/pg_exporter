# Internal Collectors

The `internal` collector module provides self-monitoring capabilities for pg_exporter itself. Unlike other collectors that monitor PostgreSQL, these collectors monitor the exporter's own health and performance.

## Why Self-Monitoring?

When running in production, you need visibility into the exporter itself:

- **Resource Usage**: Is the exporter leaking memory? Using too much CPU?
- **Performance**: Which collectors are slow? Are scrapes failing?
- **Cardinality**: How many metrics are being exported? (Critical for Cortex/Mimir with series limits)

## Architecture

The internal collector consists of two sub-collectors:

### 1. ProcessCollector (`process.rs`)

Monitors the exporter's process resource consumption.

**Metrics:**
- `pg_exporter_process_cpu_seconds_total` - Total CPU time (Counter)
- `pg_exporter_process_resident_memory_bytes` - RAM usage (Gauge)
- `pg_exporter_process_virtual_memory_bytes` - Virtual memory size (Gauge)
- `pg_exporter_process_threads` - Thread count (Gauge)
- `pg_exporter_process_open_fds` - Open file descriptors, Linux only (Gauge)
- `pg_exporter_process_start_time_seconds` - Process start time (Gauge)

**Implementation:**
- Uses the `sysinfo` crate to read process info from the OS
- Linux: Reads `/proc/$PID/stat`, `/proc/$PID/status`, `/proc/$PID/fd/`
- Cached `System` object protected by `parking_lot::Mutex`
- Collection time: ~1-5ms

### 2. ScraperCollector (`scraper.rs`)

Tracks scrape performance and health across all collectors.

**Metrics:**
- `pg_exporter_collector_scrape_duration_seconds{collector}` - Histogram with percentiles
- `pg_exporter_collector_scrape_errors_total{collector}` - Error counter per collector
- `pg_exporter_collector_last_scrape_timestamp_seconds{collector}` - Last scrape timestamp
- `pg_exporter_collector_last_scrape_success{collector}` - Success indicator (1/0)
- `pg_exporter_metrics_total` - ⭐ Total metrics exported (for cardinality monitoring)
- `pg_exporter_scrapes_total` - Total scrapes performed

**Implementation:**
- RAII `ScrapeTimer` for automatic duration recording
- `parking_lot::RwLock` for concurrent read access
- Histogram buckets: 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s

## Why Standard Library Instead of parking_lot?

We use `std::sync::{Mutex, RwLock}` instead of external crates like `parking_lot`:

### 1. No External Dependencies

```rust
use std::sync::Mutex;

// Handle lock poisoning explicitly
let guard = match mutex.lock() {
    Ok(guard) => guard,
    Err(poisoned) => {
        warn!("Mutex poisoned, recovering");
        poisoned.into_inner()
    }
};
```

This is explicit and self-documenting. The code clearly shows how we handle panics.

### 2. Educational Value

Demonstrates proper `PoisonError` handling:
- Shows awareness of panic safety
- Teaches recovery patterns
- No hidden behavior from external libraries

### 3. Minimal Dependencies

Since internal metrics are supplementary (not core PostgreSQL monitoring):
- Zero dependencies for a "nice-to-have" feature
- Simpler dependency tree
- Easier to audit

### 4. Lock Usage Benefits

- **RwLock** allows multiple concurrent readers (Prometheus scrapes)
- **Mutex** protects the cached System object
- Both handle poisoning with the `.into_inner()` pattern
- Performance is identical for our use case (low contention)

## Usage

The internal collector is **disabled by default**. Enable it explicitly:

```bash
pg_exporter --dsn postgresql://localhost/postgres --collector.internal
# Automatically exports pg_exporter_process_* and pg_exporter_collector_* metrics
```

Disable with:

```bash
# Internal metrics not needed - disabled by default
pg_exporter --dsn postgresql://localhost/postgres
```

## Prometheus Queries

### CPU Usage (%)
```promql
rate(pg_exporter_process_cpu_seconds_total[5m]) * 100
```

### Memory Usage (MB)
```promql
pg_exporter_process_resident_memory_bytes / 1024 / 1024
```

### Slowest Collector (p99 latency)
```promql
topk(5,
  histogram_quantile(0.99,
    rate(pg_exporter_collector_scrape_duration_seconds_bucket[5m])
  )
) by (collector)
```

### Failed Collectors
```promql
sum by (collector) (
  rate(pg_exporter_collector_scrape_errors_total[5m])
) > 0
```

### Total Metric Cardinality (for Cortex/Mimir limits)
```promql
pg_exporter_metrics_total
```

### Stale Collectors (not scraped in 2 minutes)
```promql
time() - pg_exporter_collector_last_scrape_timestamp_seconds{collector!=""} > 120
```

## Alerting Examples

```yaml
# High memory usage
- alert: ExporterHighMemory
  expr: pg_exporter_process_resident_memory_bytes > 500 * 1024 * 1024
  for: 5m
  annotations:
    summary: "pg_exporter using >500MB RAM"

# High CPU usage
- alert: ExporterHighCPU
  expr: rate(pg_exporter_process_cpu_seconds_total[5m]) > 0.5
  for: 5m
  annotations:
    summary: "pg_exporter using >50% CPU"

# Slow collector
- alert: SlowCollector
  expr: |
    histogram_quantile(0.99,
      rate(pg_exporter_collector_scrape_duration_seconds_bucket[5m])
    ) > 1.0
  annotations:
    summary: "Collector {{ $labels.collector }} p99 latency >1s"

# Metric cardinality explosion
- alert: HighMetricCardinality
  expr: pg_exporter_metrics_total > 10000
  annotations:
    summary: "Exporting {{ $value }} metrics (may hit Cortex limits)"

# Failed collector
- alert: CollectorFailing
  expr: rate(pg_exporter_collector_scrape_errors_total[5m]) > 0
  annotations:
    summary: "Collector {{ $labels.collector }} is failing"
```

## Grafana Dashboard Panels

Add these panels to monitor the exporter:

1. **CPU Usage** - `rate(pg_exporter_process_cpu_seconds_total[5m]) * 100`
   - Unit: percent (0-100)
   - Thresholds: Yellow >50%, Red >80%

2. **Memory Usage** - `pg_exporter_process_resident_memory_bytes`
   - Unit: bytes
   - Alert on steady growth (leak detection)

3. **Collector Latency Heatmap** - Histogram quantiles by collector
   - Shows which collectors are slow

4. **Metric Cardinality** - `pg_exporter_metrics_total`
   - Track against your Cortex/Mimir series limits

5. **Failed Collectors** - `rate(pg_exporter_collector_scrape_errors_total[5m])`
   - Alert if any collector has errors

## Platform Support

| Platform | CPU | Memory | Threads | File Descriptors |
|----------|-----|--------|---------|------------------|
| Linux | ✅ | ✅ | ✅ | ✅ |
| macOS | ✅ | ✅ | ⚠️ Fallback | ❌ Not available |
| Windows | ✅ | ✅ | ⚠️ Fallback | ❌ Not available |

Platform-specific code is guarded with `#[cfg(target_os = "linux")]`.

## Performance Impact

- **CPU**: <0.1% additional overhead
- **Memory**: ~10KB for cached `System` object
- **Collection time**: ~1-5ms per scrape
- **Lock contention**: Minimal (scrapes happen every 15-60 seconds)

## Comparison with `scripts/monitor-exporter.sh`

| Feature | Internal Metrics | Bash Script |
|---------|-----------------|-------------|
| Accuracy | ✅ Same (reads /proc) | ✅ Same |
| Sampling Rate | 15-60s (scrape interval) | 1-5s (configurable) |
| Historical Data | ✅ In Prometheus | ❌ Point-in-time only |
| Alerting | ✅ Prometheus alerts | ❌ Manual monitoring |
| Use Case | Production monitoring | Debugging/troubleshooting |

**Both are complementary!** Use internal metrics for production monitoring and alerts. Use the script for high-frequency debugging during incidents.

## Testing

Run tests:

```bash
cargo test --lib internal
```

Output:
```
running 9 tests
test collectors::internal::process::tests::test_process_collector_new ... ok
test collectors::internal::process::tests::test_process_collector_registers_without_error ... ok
test collectors::internal::process::tests::test_process_collector_collects_stats ... ok
test collectors::internal::scraper::tests::test_scraper_collector_new ... ok
test collectors::internal::scraper::tests::test_scraper_collector_registers_without_error ... ok
test collectors::internal::scraper::tests::test_scrape_timer_records_duration ... ok
test collectors::internal::scraper::tests::test_scrape_timer_records_error ... ok
test collectors::internal::scraper::tests::test_update_metrics_count ... ok
test collectors::internal::scraper::tests::test_increment_scrapes ... ok

test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured
```

## References

- [Process Collector Source](process.rs)
- [Scraper Collector Source](scraper.rs)
## Dependencies

The internal collector only requires one external dependency:

- **sysinfo = "0.37"** - Cross-platform system information library
  - Used to read process stats from the OS
  - Well-maintained, widely used
  - Platform-specific implementations (Linux: /proc, macOS: proc_pidinfo, etc.)

All synchronization primitives use standard library (`std::sync`).
