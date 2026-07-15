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
