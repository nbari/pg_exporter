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
