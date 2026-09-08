use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};

#[test]
fn production_collectors_do_not_bypass_connection_budget() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let collector_root = root.join("src").join("collectors");
    let mut failures = Vec::new();

    for path in rust_files_under(&collector_root)? {
        let source = std::fs::read_to_string(&path)?;
        let production_source = source
            .split("#[cfg(test)]")
            .next()
            .unwrap_or(source.as_str());
        let relative = path.strip_prefix(root).unwrap_or(path.as_path());
        let is_util = relative == Path::new("src/collectors/util.rs");
        let is_registry = relative == Path::new("src/collectors/registry.rs");

        if !is_util && production_source.contains("PgConnection::connect") {
            failures.push(format!(
                "{} opens PgConnection directly; use util::open_db_connection with a permit",
                relative.display()
            ));
        }

        if !is_util && !is_registry && production_source.contains("Semaphore::new(") {
            failures.push(format!(
                "{} creates a local semaphore; use util::acquire_db_query_permit for per-database work",
                relative.display()
            ));
        }

        if !is_registry && production_source.contains("PgPoolOptions::new") {
            failures.push(format!(
                "{} creates a collector-side pool; collectors must use the shared pool or ephemeral util connections",
                relative.display()
            ));
        }

        if !is_util
            && !is_registry
            && (production_source.contains("tokio::time::timeout(")
                || production_source.contains("time::timeout(")
                || production_source.contains("use tokio::time::timeout")
                || production_source.contains("time::timeout,"))
        {
            failures.push(format!(
                "{} uses a client-side timeout in collector code; rely on PostgreSQL statement_timeout for query execution so backend work ends before scrape cleanup",
                relative.display()
            ));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(failures.join("\n")))
    }
}

#[test]
fn production_collectors_do_not_use_regexp_replace() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let collector_root = root.join("src").join("collectors");
    let mut failures = Vec::new();

    for path in rust_files_under(&collector_root)? {
        let source = std::fs::read_to_string(&path)?;
        let production_source = source
            .split("#[cfg(test)]")
            .next()
            .unwrap_or(source.as_str());

        if production_source
            .to_ascii_lowercase()
            .contains("regexp_replace(")
        {
            let relative = path.strip_prefix(root).unwrap_or(path.as_path());
            failures.push(format!(
                "{} uses regexp_replace in collector code; avoid per-row regex processing in scrape queries",
                relative.display()
            ));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(failures.join("\n")))
    }
}

#[test]
fn open_db_connection_has_bounded_connect_timeout() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let util_path = root.join("src").join("collectors").join("util.rs");
    let source = std::fs::read_to_string(&util_path)?;
    let (_, tail) = source
        .split_once("pub async fn open_db_connection")
        .ok_or_else(|| anyhow!("open_db_connection not found in {}", util_path.display()))?;
    let function_source = tail
        .split("#[cfg(test)]")
        .next()
        .ok_or_else(|| anyhow!("failed to isolate open_db_connection source"))?;

    assert!(
        function_source.contains("get_connect_timeout()"),
        "open_db_connection must read the configured connect timeout"
    );
    assert!(
        function_source.contains("timeout(connect_timeout, PgConnection::connect_with(&opts))"),
        "open_db_connection must bound PgConnection::connect_with with the connect timeout"
    );

    Ok(())
}

#[test]
fn shared_pool_uses_the_connection_budget_constant() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let exporter_path = root.join("src").join("exporter").join("mod.rs");
    let source = std::fs::read_to_string(&exporter_path)?;

    assert!(
        source.contains(".max_connections(SHARED_POOL_MAX_CONNECTIONS)"),
        "the shared pool must use SHARED_POOL_MAX_CONNECTIONS so the documented budget cannot drift"
    );
    assert!(
        !source.contains(".max_connections(3)"),
        "do not duplicate the shared pool size as a literal"
    );

    Ok(())
}

/// Query-text resolution can materialize and spill the entire `pg_stat_statements` text
/// corpus, so `collect()` must only schedule it and never await it on the scrape path.
#[test]
fn statements_query_text_lookup_stays_detached_from_collect() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let statements_path = root
        .join("src")
        .join("collectors")
        .join("statements")
        .join("pg_statements.rs");
    let source = std::fs::read_to_string(&statements_path)?;

    let (_, collector_impl) = source
        .split_once("impl Collector for PgStatementsCollector")
        .ok_or_else(|| anyhow!("PgStatementsCollector Collector impl not found"))?;
    let (_, collect_tail) = collector_impl
        .split_once("fn collect")
        .ok_or_else(|| anyhow!("PgStatementsCollector::collect not found"))?;
    let collect_source = collect_tail
        .split_once("fn enabled_by_default")
        .map_or(collect_tail, |(collect, _)| collect);

    assert!(
        collect_source.contains("self.schedule_query_text_refresh(pool, missing);"),
        "PgStatementsCollector::collect must dispatch query-text resolution without awaiting it"
    );
    assert!(
        !collect_source.contains("refresh_query_texts("),
        "PgStatementsCollector::collect must not call the query-text lookup directly"
    );

    let (_, scheduler_tail) = source
        .split_once("fn schedule_query_text_refresh")
        .ok_or_else(|| anyhow!("schedule_query_text_refresh not found"))?;
    let scheduler_source = scheduler_tail
        .split_once("fn should_warn_work_mem_refusal")
        .map_or(scheduler_tail, |(scheduler, _)| scheduler);

    assert!(
        scheduler_source.contains("tokio::spawn(task)"),
        "schedule_query_text_refresh must detach the lookup with tokio::spawn"
    );

    Ok(())
}

/// Issue #35: several collectors read the operating system with blocking, synchronous
/// APIs — `std::fs` on `/proc`, `sysctlbyname`, `sysinfo` refreshes, reading a certificate
/// off disk. Running any of that inline in `collect_once` occupies a Tokio worker for the
/// whole read, which stops the `sqlx` pool's futures from being polled and makes unrelated
/// collectors fail with a badly misleading `pool timed out while waiting for an open
/// connection`.
///
/// Any `collect_once` in a file that touches the OS must therefore hand the work to
/// `blocking::offload` rather than calling its sampler directly. This covers all of
/// `src/collectors/`, not just the `system` collector where the problem was found.
#[test]
fn collectors_do_not_block_the_runtime_with_os_reads() -> Result<()> {
    /// Source-text markers for blocking operating-system access.
    const OS_READ_MARKERS: [&str; 7] = [
        "std::fs::",
        "fs::read(",
        "fs::read_to_string",
        "sysinfo",
        "sysctlbyname",
        "refresh_processes",
        "refresh_memory",
    ];

    /// Files known to sample the OS today. Listed only so this test cannot quietly decay
    /// into a no-op if a marker above stops matching; a new collector is covered
    /// automatically and does not need to be added here.
    const KNOWN_OS_COLLECTORS: [&str; 5] = [
        "src/collectors/exporter/process.rs",
        "src/collectors/system/cpu.rs",
        "src/collectors/system/memory.rs",
        "src/collectors/system/process.rs",
        "src/collectors/tls/certificate.rs",
    ];

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let collector_root = root.join("src").join("collectors");
    let mut failures = Vec::new();
    let mut checked: Vec<String> = Vec::new();

    for path in rust_files_under(&collector_root)? {
        let source = std::fs::read_to_string(&path)?;
        let production_source = source
            .split("#[cfg(test)]")
            .next()
            .unwrap_or(source.as_str());
        let relative = path.strip_prefix(root).unwrap_or(path.as_path());

        let Some(collect_once) = production_source.split("fn collect_once").nth(1) else {
            continue;
        };

        // Match real code only: the doc comments discuss `/proc` and `sysinfo` at length
        // by design, and an umbrella that merely documents them does no I/O itself.
        let touches_os = production_source
            .lines()
            .map(str::trim)
            .filter(|line| !line.starts_with("//"))
            .any(|line| OS_READ_MARKERS.iter().any(|marker| line.contains(marker)));
        if !touches_os {
            continue;
        }
        checked.push(relative.to_string_lossy().into_owned());

        // Bound the slice to the body of collect_once, which ends at the next item.
        let body = collect_once
            .split("\n    fn ")
            .next()
            .unwrap_or(collect_once);

        if !body.contains("blocking::offload") {
            failures.push(format!(
                "{} runs collect_once without blocking::offload: synchronous OS reads must go \
                 to the blocking pool, or one slow read starves every other collector and \
                 surfaces as a bogus `pool timed out` (issue #35)",
                relative.display()
            ));
        }
    }

    for expected in KNOWN_OS_COLLECTORS {
        if !checked.iter().any(|path| path == expected) {
            failures.push(format!(
                "{expected} is no longer recognised as sampling the OS: either it genuinely \
                 stopped (update KNOWN_OS_COLLECTORS) or OS_READ_MARKERS stopped matching it, \
                 which would silently stop enforcing issue #35 for every collector"
            ));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(failures.join("\n")))
    }
}

/// The `system.process` collector accumulates a monotonic CPU counter from per-PID deltas,
/// so the order of "sample" and "publish the new baseline" is load-bearing.
///
/// Two collections can overlap: the scrape gate reopens the moment a scrape times out
/// (issue #34) and a `spawn_blocking` sample that has already started cannot be aborted, so
/// an abandoned scrape's walk keeps running alongside the next one. If the sample happened
/// outside the baseline lock, the newer pass could publish its baseline first, the older
/// pass would then count no delta and overwrite the baseline with its own lower totals, and
/// the pass after that would re-count the interval between them — inflating
/// `pg_system_process_group_cpu_seconds_total` above the CPU actually consumed.
///
/// This is a source-order assertion rather than a race reproduction: the interleaving needs
/// a pause between the sample and the lock, which cannot be injected from outside, and a
/// timing-based test would be flaky in both directions.
#[test]
fn process_group_locks_the_cpu_baseline_before_sampling() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let process_rs = root
        .join("src")
        .join("collectors")
        .join("system")
        .join("process.rs");
    let source = std::fs::read_to_string(&process_rs)?;
    let production_source = source
        .split("#[cfg(test)]")
        .next()
        .unwrap_or(source.as_str());

    let body = production_source
        .split("fn collect_stats")
        .nth(1)
        .ok_or_else(|| anyhow!("process.rs no longer has a collect_stats to check"))?
        .split("\n    }")
        .next()
        .unwrap_or_default();

    let lock_at = body
        .find("self.prev_cpu.lock()")
        .ok_or_else(|| anyhow!("collect_stats no longer locks prev_cpu"))?;
    let sample_at = body
        .find("sample_processes(")
        .ok_or_else(|| anyhow!("collect_stats no longer calls sample_processes"))?;

    if lock_at > sample_at {
        return Err(anyhow!(
            "collect_stats samples before taking the prev_cpu lock: two overlapping \
             collections can then publish their baselines out of order and \
             pg_system_process_group_cpu_seconds_total over-reports (issues #34, #35)"
        ));
    }

    Ok(())
}

/// Issue #35: `/proc/<pid>/smaps_rollup` makes the kernel walk every page-table entry of
/// every mapping, costing `O(processes x resident pages)` — 13.9s of a 15s scrape budget on
/// a production primary. It must stay reachable only through the explicit
/// `--system.process-memory=pss` opt-in, never from a default code path.
#[test]
fn smaps_rollup_stays_behind_the_pss_opt_in() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let process_rs = root
        .join("src")
        .join("collectors")
        .join("system")
        .join("process.rs");
    let source = std::fs::read_to_string(&process_rs)?;
    let production_source = source
        .split("#[cfg(test)]")
        .next()
        .unwrap_or(source.as_str());

    // Only real code: doc comments discuss smaps_rollup at length by design.
    let code_lines = || {
        production_source
            .lines()
            .map(str::trim)
            .filter(|line| !line.starts_with("//"))
    };

    let readers: Vec<&str> = code_lines()
        .filter(|line| line.contains("/proc/{pid}/smaps_rollup"))
        .collect();

    if readers.len() != 1 {
        return Err(anyhow!(
            "expected exactly one /proc/<pid>/smaps_rollup read in process.rs (inside \
             read_pss_bytes), found {}: {readers:?}",
            readers.len()
        ));
    }

    // read_pss_bytes must only ever be handed to the dispatcher as a lazy `|| read_pss_bytes(pid)`
    // thunk. Called eagerly — passed as a value, or read before the match — every scrape
    // would pay for the walk regardless of which source was configured.
    for call in code_lines()
        .filter(|line| line.contains("read_pss_bytes(") && !line.contains("fn read_pss_bytes"))
    {
        if !call.starts_with("|| read_pss_bytes(pid)") {
            return Err(anyhow!(
                "unexpected read_pss_bytes call site '{call}': PSS must stay a lazy thunk \
                 reached only via --system.process-memory=pss, never evaluated eagerly \
                 (issue #35)"
            ));
        }
    }

    // The dispatch itself — that the RSS arm never calls the PSS reader — is asserted
    // behaviourally by `rss_mode_never_pays_for_the_smaps_rollup_walk` in process.rs, which
    // counts calls to an injected reader. This only pins the laziness the counter relies on.
    if !production_source.contains("ProcessMemorySource::Rss => rss()") {
        return Err(anyhow!(
            "process.rs no longer dispatches the default RSS source to the rss reader; PSS must \
             not become the default again (issue #35)"
        ));
    }

    Ok(())
}

/// Enforces the collector module layout: `src/collectors/<name>/mod.rs` must be
/// a thin **entry point / umbrella** that wires up sub-collectors, not the place
/// where metrics and SQL live. The real implementation belongs in a sibling file
/// named after the source view (for example `stat_io/pg_stat_io.rs`,
/// `statements/pg_statements.rs`, `stat/user_tables.rs`).
///
/// This catches the common mistake of dumping an entire collector into `mod.rs`.
#[test]
fn collector_mod_rs_is_a_thin_umbrella() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let collector_root = root.join("src").join("collectors");

    // Signals that a file contains collector *implementation* (metric definitions
    // or SQL) rather than just wiring. None of these belong in an umbrella mod.rs.
    let forbidden: [&str; 10] = [
        "IntGaugeVec::new",
        "GaugeVec::new",
        "IntCounterVec::new",
        "CounterVec::new",
        "Opts::new(",
        "registry.register(Box::new(",
        ".with_label_values(",
        "sqlx::query",
        "fetch_all(",
        "fetch_optional(",
    ];

    let mut failures = Vec::new();

    for path in rust_files_under(&collector_root)? {
        // Only per-collector directory entry points: src/collectors/<name>/mod.rs.
        // Skip the top-level src/collectors/mod.rs (the registration hub).
        let is_mod_rs = path.file_name().is_some_and(|name| name == "mod.rs");
        let is_collector_dir_mod =
            path.parent().and_then(Path::parent) == Some(collector_root.as_path());
        if !is_mod_rs || !is_collector_dir_mod {
            continue;
        }

        let source = std::fs::read_to_string(&path)?;
        let production_source = source
            .split("#[cfg(test)]")
            .next()
            .unwrap_or(source.as_str());
        let relative = path.strip_prefix(root).unwrap_or(path.as_path());

        for marker in forbidden {
            if production_source.contains(marker) {
                failures.push(format!(
                    "{} contains `{marker}`: collector mod.rs must stay a thin umbrella. \
                     Move metrics/SQL into a sibling file (e.g. `pg_stat_io.rs`) and have \
                     mod.rs only declare the submodule and fan out to it \
                     (see statements/pg_statements.rs)",
                    relative.display()
                ));
            }
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(failures.join("\n")))
    }
}

fn rust_files_under(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];

    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();
            let metadata = entry.metadata()?;

            if metadata.is_dir() {
                pending.push(entry_path);
            } else if entry_path.extension().is_some_and(|ext| ext == "rs") {
                files.push(entry_path);
            }
        }
    }

    files.sort();
    Ok(files)
}
