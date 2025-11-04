//! Shared utilities for collectors:
//! - Global, read-only exclusion list of databases (set once at startup).
//! - Parsed base connect options derived from the DSN to build per-database connections.
//! - Cached tiny PgPools per non-default database (reuse across scrapes).

use anyhow::{Result, anyhow};
use once_cell::sync::OnceCell;
use secrecy::{ExposeSecret, SecretString};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{collections::HashMap, str::FromStr, sync::Arc};
use tokio::sync::RwLock;

/// Global holder for excluded databases, set once at startup via CLI/env.
static EXCLUDED: OnceCell<Arc<[String]>> = OnceCell::new();

/// Parsed base connect options derived from the provided DSN (set once).
static BASE_OPTS: OnceCell<PgConnectOptions> = OnceCell::new();

/// Default database name parsed from DSN.
static DEFAULT_DB: OnceCell<String> = OnceCell::new();

/// Cache of per-database tiny pools (only for non-default DBs).
static POOLS: OnceCell<RwLock<HashMap<String, PgPool>>> = OnceCell::new();

/// PostgreSQL version number (e.g., 140000 for v14.0, 170000 for v17.0).
static PG_VERSION: OnceCell<i32> = OnceCell::new();

/// Set the excluded databases from CLI/env. Call this once during startup.
pub fn set_excluded_databases(list: Vec<String>) {
    let mut cleaned: Vec<String> = list
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    cleaned.dedup();
    let _ = EXCLUDED.set(Arc::from(cleaned));
}

/// Get the excluded databases as a static slice.
#[inline]
pub fn get_excluded_databases() -> &'static [String] {
    match EXCLUDED.get() {
        Some(arc) => &arc[..],
        None => &[],
    }
}

/// Convenience check: is a given database name excluded?
#[inline]
pub fn is_database_excluded(datname: &str) -> bool {
    get_excluded_databases().iter().any(|d| d == datname)
}

/// Set the PostgreSQL version. Call this once during startup after connecting.
pub fn set_pg_version(version: i32) {
    let _ = PG_VERSION.set(version);
}

/// Get the PostgreSQL version number.
/// Returns 0 if not set (should never happen in production).
#[inline]
pub fn get_pg_version() -> i32 {
    PG_VERSION.get().copied().unwrap_or(0)
}

/// Check if PostgreSQL version is at least the specified minimum.
#[inline]
pub fn is_pg_version_at_least(min_version: i32) -> bool {
    get_pg_version() >= min_version
}

/// Initialize (idempotent) the base connect options from the provided DSN (SecretString).
/// Also records the default database name and initializes the pool cache.
pub fn set_base_connect_options_from_dsn(dsn: &SecretString) -> Result<()> {
    if BASE_OPTS.get().is_none() {
        let opts = PgConnectOptions::from_str(dsn.expose_secret())?;
        let _ = BASE_OPTS.set(opts.clone());

        // Record default database name if present, else fallback to "postgres".
        // PgConnectOptions::get_database() returns Option<&str>.
        let dbname = opts.get_database().unwrap_or("postgres").to_string();
        let _ = DEFAULT_DB.set(dbname);
    }

    if POOLS.get().is_none() {
        let _ = POOLS.set(RwLock::new(HashMap::new()));
    }

    Ok(())
}

/// Returns the default database name derived from the DSN, if available.
#[inline]
pub fn get_default_database() -> Option<&'static str> {
    DEFAULT_DB.get().map(|s| s.as_str())
}

/// Build connect options for a specific database name based on the base DSN.
pub fn connect_options_for_db(datname: &str) -> Result<PgConnectOptions> {
    let base = BASE_OPTS.get().cloned().ok_or_else(|| {
        anyhow!("BASE_OPTS not set; call set_base_connect_options_from_dsn() at startup")
    })?;
    Ok(base.database(datname))
}

/// Get (or create) a tiny pool for the specified database. Only used for non-default DBs.
/// The default DB should reuse the shared pool created at startup.
pub async fn get_or_create_pool_for_db(datname: &str) -> Result<PgPool> {
    // Do not create a new pool for the default database.
    if let Some(def) = get_default_database()
        && def == datname
    {
        return Err(anyhow!(
            "get_or_create_pool_for_db called for default database; use shared pool"
        ));
    }

    let pools = POOLS.get().ok_or_else(|| {
        anyhow!("Pool cache not initialized; call set_base_connect_options_from_dsn()")
    })?;

    // Fast path: check cache
    {
        let guard = pools.read().await;
        if let Some(pool) = guard.get(datname) {
            return Ok(pool.clone());
        }
    }

    // Create tiny pool for this DB
    let opts = connect_options_for_db(datname)?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect_with(opts)
        .await?;

    let mut guard = pools.write().await;
    guard.insert(datname.to_string(), pool.clone());

    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get_exclusions() {
        set_excluded_databases(vec![
            "postgres".into(),
            "template0".into(),
            "template0".into(), // duplicate
            " ".into(),         // empty after trim
        ]);

        let got = get_excluded_databases();
        assert_eq!(got, &["postgres".to_string(), "template0".to_string()]);
        assert!(is_database_excluded("postgres"));
        assert!(!is_database_excluded("not_there"));
    }

    #[test]
    fn test_pg_version_utilities() {
        // Test default (not set)
        assert_eq!(get_pg_version(), 0);
        assert!(!is_pg_version_at_least(140000));

        // Test setting version
        set_pg_version(160000); // PostgreSQL 16
        assert_eq!(get_pg_version(), 160000);
        assert!(is_pg_version_at_least(140000)); // >= 14
        assert!(is_pg_version_at_least(160000)); // >= 16
        assert!(!is_pg_version_at_least(170000)); // < 17
    }
}
