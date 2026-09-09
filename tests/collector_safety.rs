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

/// Source-text markers for blocking operating-system access.
const OS_READ_MARKERS: [&str; 10] = [
    "std::fs::",
    "::read(",
    "::read_dir(",
    "::read_to_string(",
    "File::open(",
    "OpenOptions::new(",
    "sysinfo",
    "sysctlbyname",
    "refresh_processes",
    "refresh_memory",
];

/// Import roots whose items block the calling thread when invoked. Used only to notice
/// that a `use` renamed one of them, which would hide every later call from the literal
/// markers above.
const OS_IMPORT_ROOTS: [&str; 4] = ["std::fs", "std::io", "sysinfo", "libc"];

/// Issue #35: several collectors read the operating system with blocking, synchronous
/// APIs — `std::fs` on `/proc`, `sysctlbyname`, `sysinfo` refreshes, reading a certificate
/// off disk. Running any of that inline in `collect_once` occupies a Tokio worker for the
/// whole read, which stops the `sqlx` pool's futures from being polled and makes unrelated
/// collectors fail with a badly misleading `pool timed out while waiting for an open
/// connection`.
///
/// Any `collect_once` in a file that touches the OS must therefore hand the work to
/// `blocking::offload_coalesced` rather than calling its sampler directly. This covers all of
/// `src/collectors/`, not just the `system` collector where the problem was found.
///
/// # Why this is a call-graph check and not a line match
///
/// Matching OS markers only against the literal text of `collect_once` is trivially
/// defeated by the most natural refactor there is: move the read one level down into a
/// helper and call the helper. The helper still runs on the runtime worker, the marker no
/// longer appears in `collect_once`, and the `offload_coalesced` call sitting next to it
/// still satisfies a text check — so the guard passes while issue #35 is back.
///
/// So every function in the file is classified first: a function is *blocking* if it
/// contains an OS marker, or if it calls a blocking function. `collect_once` may then
/// reference a blocking function only from inside the argument list of
/// `blocking::offload_coalesced(..)`, which is the one place the work does not run on a
/// runtime worker. Renamed imports (`use std::fs::read_to_string as slurp;`) contribute
/// their alias as an extra marker, so aliasing does not hide a read either.
///
/// Scope: within one file. A sampler in a *different* module reached from `collect_once`
/// is not traced — but that module is itself scanned by this test, so an OS read there
/// still has to sit behind its own offload.
#[test]
fn collectors_do_not_block_the_runtime_with_os_reads() -> Result<()> {
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

        let markers = os_read_markers(production_source);

        // Match real code only: the doc comments discuss `/proc` and `sysinfo` at length
        // by design, and an umbrella that merely documents them does no I/O itself.
        let touches_os = production_source
            .lines()
            .map(str::trim)
            .filter(|line| !line.starts_with("//"))
            .any(|line| markers.iter().any(|marker| line.contains(marker.as_str())));
        if !touches_os {
            continue;
        }
        checked.push(relative.to_string_lossy().into_owned());

        // Bound the slice to the body of collect_once, which ends at the next item.
        let body = collect_once
            .split("\n    fn ")
            .next()
            .unwrap_or(collect_once);

        let body_lines: Vec<&str> = body
            .lines()
            .map(str::trim)
            .filter(|line| !line.starts_with("//"))
            .collect();

        if !body_lines
            .iter()
            .any(|line| line.contains("blocking::offload_coalesced("))
        {
            failures.push(format!(
                "{} runs collect_once without blocking::offload_coalesced: synchronous OS reads \
                 must be offloaded and capped at one submitted sample per collector, or aborted \
                 scrapes can pile work onto the blocking pool (issues #34, #35)",
                relative.display()
            ));
        }

        // Merely *mentioning* blocking::offload is not enough. Every OS read, and every
        // function that reaches one, must be handed to the offload rather than called
        // beside it.
        let blocking_fns = blocking_functions(production_source, &markers);
        let offloaded = offload_argument_lines(&body_lines);

        for (line, is_offloaded) in body_lines.iter().zip(offloaded.iter()) {
            if *is_offloaded {
                continue;
            }

            if let Some(marker) = markers.iter().find(|marker| line.contains(marker.as_str())) {
                failures.push(format!(
                    "{} reads the OS directly inside collect_once ('{marker}' in '{line}'): move \
                     the read into a named sampler reached only through \
                     blocking::offload_coalesced, or an inline slow read starves every other \
                     collector (issue #35)",
                    relative.display()
                ));
            }

            if let Some(called) = blocking_fns
                .iter()
                .find(|name| line.contains(&format!("{name}(")))
            {
                failures.push(format!(
                    "{} calls the blocking function '{called}' from collect_once outside the \
                     blocking::offload_coalesced argument list ('{line}'): it reaches an OS read, \
                     so calling it here runs that read on a Tokio worker exactly as issue #35 did. \
                     Pass it to offload_coalesced instead",
                    relative.display()
                ));
            }
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

/// The literal OS markers plus one per blocking item this file imports under a new name.
///
/// `use std::fs::read_to_string as slurp;` moves every later read behind `slurp(`, which
/// none of the literal markers match. The alias becomes a marker of its own so renaming
/// an import cannot launder a blocking read.
fn os_read_markers(production_source: &str) -> Vec<String> {
    let mut markers: Vec<String> = OS_READ_MARKERS.iter().map(|&m| m.to_string()).collect();

    for line in production_source.lines().map(str::trim) {
        if !line.starts_with("use ") || !OS_IMPORT_ROOTS.iter().any(|root| line.contains(root)) {
            continue;
        }

        for renamed in line.split(" as ").skip(1) {
            let alias: String = renamed
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !alias.is_empty() {
                markers.push(format!("{alias}("));
            }
        }
    }

    markers
}

/// Names of every function in `production_source` that performs a blocking OS read, or
/// calls something that does.
///
/// Attribution is by position: a line belongs to the most recent `fn` above it, which is
/// how Rust source reads and is enough to tell "this helper does the read" from "this
/// helper is merely named next to it". Propagation runs to a fixpoint, so a chain of
/// helpers is as blocking as the read at the end of it.
fn blocking_functions(production_source: &str, markers: &[String]) -> Vec<String> {
    let mut spans: Vec<(String, Vec<&str>)> = Vec::new();

    for line in production_source.lines().map(str::trim) {
        if let Some(name) = function_name(line) {
            spans.push((name, Vec::new()));
        }
        if let Some((_, lines)) = spans.last_mut() {
            lines.push(line);
        }
    }

    let code_lines = |lines: &Vec<&str>| -> Vec<String> {
        lines
            .iter()
            .filter(|line| !line.starts_with("//"))
            .map(|line| (*line).to_string())
            .collect()
    };

    let mut blocking: Vec<String> = Vec::new();
    for (name, lines) in &spans {
        let reads_os = code_lines(lines)
            .iter()
            .any(|line| markers.iter().any(|marker| line.contains(marker.as_str())));
        if reads_os && !blocking.contains(name) {
            blocking.push(name.clone());
        }
    }

    loop {
        let mut discovered: Vec<String> = Vec::new();

        for (name, lines) in &spans {
            if blocking.contains(name) || discovered.contains(name) {
                continue;
            }
            let calls_blocking = code_lines(lines).iter().any(|line| {
                blocking
                    .iter()
                    .any(|callee| line.contains(&format!("{callee}(")))
            });
            if calls_blocking {
                discovered.push(name.clone());
            }
        }

        if discovered.is_empty() {
            return blocking;
        }
        blocking.extend(discovered);
    }
}

/// The name defined by a function-definition line, if the line is one.
fn function_name(trimmed_line: &str) -> Option<String> {
    let (prefix, rest) = trimmed_line.split_once("fn ")?;

    // `fn` may only be preceded by qualifiers; anything else is prose or a type bound.
    let qualifiers_only = prefix.split_whitespace().all(|word| {
        matches!(
            word,
            "pub" | "async" | "const" | "unsafe" | "extern" | "default"
        ) || word.starts_with("pub(")
    });
    if !qualifiers_only {
        return None;
    }

    let name: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    (!name.is_empty()).then_some(name)
}

/// Marks the lines of `body_lines` that sit inside a `blocking::offload_coalesced(..)`
/// argument list, tracking parenthesis depth so a multi-line call is covered exactly.
///
/// Work named there runs on the blocking pool; work named anywhere else in `collect_once`
/// runs on a Tokio worker.
fn offload_argument_lines(body_lines: &[&str]) -> Vec<bool> {
    let mut inside = Vec::with_capacity(body_lines.len());
    let mut depth: usize = 0;

    for line in body_lines {
        let opens = line.matches('(').count();
        let closes = line.matches(')').count();

        if depth == 0 {
            if line.contains("offload_coalesced(") {
                depth = opens.saturating_sub(closes);
                inside.push(true);
            } else {
                inside.push(false);
            }
            continue;
        }

        inside.push(true);
        depth = depth.saturating_add(opens).saturating_sub(closes);
    }

    inside
}

/// The `system.process` collector accumulates a monotonic CPU counter from per-PID deltas,
/// so the order of "sample" and "publish the new baseline" is load-bearing.
///
/// The one-slot submission guard prevents normal scrape-driven overlap. The baseline lock is
/// still a necessary defence for direct/internal calls and future refactors: if a sample
/// happened outside it, a newer pass could publish its baseline first, the older pass would
/// then count no delta and overwrite the baseline with its own lower totals, and the pass
/// after that would re-count the interval between them — inflating
/// `pg_system_process_group_cpu_seconds_total` above the CPU actually consumed.
///
/// The lock is a `try_lock`: an overlapping direct collection skips rather than queueing. The
/// skip path is pinned behaviourally by
/// `a_concurrent_collection_skips_instead_of_interleaving_samples` in process.rs; this guard
/// pins the shape that behaviour depends on. The outer slot provides the blocking-pool bound.
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

    // The sampling/publishing implementation: the lock must be taken before the sampler
    // runs, and held across it.
    let body = production_source
        .split("fn collect_stats_with")
        .nth(1)
        .ok_or_else(|| anyhow!("process.rs no longer has a collect_stats_with to check"))?
        .split("\n    }")
        .next()
        .unwrap_or_default();

    let lock_at = body
        .find("self.prev_cpu.try_lock()")
        .ok_or_else(|| anyhow!("collect_stats_with must take prev_cpu with try_lock"))?;
    let sample_at = body
        .find("let samples = sample();")
        .ok_or_else(|| anyhow!("collect_stats_with no longer samples through its closure"))?;

    if lock_at > sample_at {
        return Err(anyhow!(
            "collect_stats_with samples before taking the prev_cpu lock: two overlapping \
             collections can then publish their baselines out of order and \
             pg_system_process_group_cpu_seconds_total over-reports (issues #34, #35)"
        ));
    }

    // The production sampler must reach that implementation: collect_stats dispatches the
    // per-platform `sample_processes` calls into it.
    let dispatch = production_source
        .split("fn collect_stats(")
        .nth(1)
        .ok_or_else(|| anyhow!("process.rs no longer has a collect_stats to check"))?
        .split("\n    }")
        .next()
        .unwrap_or_default();
    if !dispatch.contains("collect_stats_with(|| sample_processes(") {
        return Err(anyhow!(
            "collect_stats no longer routes sample_processes through collect_stats_with: the \
             production sampler would bypass the baseline lock (issues #34, #35)"
        ));
    }

    let code_lines = production_source
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with("//"));
    for call in code_lines
        .clone()
        .filter(|line| line.contains("sample_processes(") && !line.contains("fn sample_processes"))
    {
        if !call.starts_with("self.collect_stats_with(|| sample_processes(") {
            return Err(anyhow!(
                "unexpected process sampler call site '{call}': production sampling must happen \
                 inside collect_stats_with so prev_cpu is already locked"
            ));
        }
    }

    let sampler_uses = code_lines
        .filter(|line| line.contains("sample_processes"))
        .count();
    if sampler_uses != 4 {
        return Err(anyhow!(
            "expected exactly four sample_processes references (Linux/FreeBSD definitions and \
             their locked call sites), found {sampler_uses}"
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
        .filter(|line| line.contains("/smaps_rollup") || line.contains("\"smaps_rollup\""))
        .collect();

    if readers.len() != 1 {
        return Err(anyhow!(
            "expected exactly one smaps_rollup reference in production code (the read inside \
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

    // The identifier itself, not just the call shape: exactly one definition and one
    // lazy thunk. A function-reference detour (`let f = read_pss_bytes; f(pid)`) would
    // add an occurrence without tripping the call-site check above.
    let identifier_uses = code_lines()
        .filter(|line| line.contains("read_pss_bytes"))
        .count();
    if identifier_uses != 2 {
        return Err(anyhow!(
            "expected exactly 2 references to read_pss_bytes in production code (its \
             definition and the lazy thunk in read_memory_bytes), found {identifier_uses}: \
             the PSS walk must keep a single, visible, lazy call site (issue #35)"
        ));
    }

    // The dispatch itself — that the RSS arm never calls the PSS reader — is asserted
    // behaviourally by `rss_mode_never_pays_for_the_smaps_rollup_walk` in process.rs, which
    // counts calls to an injected reader. This only pins the laziness the counter relies on.
    if !production_source.contains("ProcessMemorySource::Rss => statm()") {
        return Err(anyhow!(
            "process.rs no longer dispatches the default RSS source to the statm reader; PSS \
             must not become the default again (issue #35)"
        ));
    }

    Ok(())
}

/// The gate must reopen when its request ends even though server-side cancellation or a
/// non-cancellable blocking sample may still be finishing. Keep the operator documentation
/// aligned with that deliberately overlapping lifetime model.
#[test]
fn scrape_gate_documentation_matches_release_and_connection_semantics() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let readme = std::fs::read_to_string(root.join("README.md"))?;
    let normalized = readme.split_whitespace().collect::<Vec<_>>().join(" ");

    if normalized
        .contains("keeps the scrape gate closed until in-flight collector work has unwound")
    {
        return Err(anyhow!(
            "README still describes the pre-#34 gate lifetime; the request-owned gate now \
             reopens before non-cancellable work and server cancellation finish"
        ));
    }
    for required in [
        "request-owned scrape gate is released immediately",
        "operational estimate, not a hard server-side bound",
        "A role limit is the hard backstop",
    ] {
        if !normalized.contains(required) {
            return Err(anyhow!(
                "README no longer documents the gate/connection overlap accurately; missing \
                 text: {required:?}"
            ));
        }
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
