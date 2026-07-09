//! Shared utilities for collectors:
//! - Global, read-only exclusion list of databases (set once at startup).
//! - Parsed base connect options derived from the DSN to build per-database connections.
//! - Ephemeral per-database connections (opened per scrape query, closed on drop) so the
//!   exporter's connection footprint tracks scrape concurrency, not the database count.

use anyhow::{Result, anyhow};
use once_cell::sync::OnceCell;
use secrecy::{ExposeSecret, SecretString};
use sqlx::Connection;
use sqlx::postgres::{PgConnectOptions, PgConnection};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::timeout,
};

/// Global holder for excluded databases, set once at startup via CLI/env.
static EXCLUDED: OnceCell<Arc<[String]>> = OnceCell::new();

/// Parsed base connect options derived from the provided DSN (set once).
static BASE_OPTS: OnceCell<PgConnectOptions> = OnceCell::new();

/// Default database name parsed from DSN.
static DEFAULT_DB: OnceCell<String> = OnceCell::new();

/// `PostgreSQL` version number (e.g., `140_000` for v14.0, `170_000` for v17.0).
static PG_VERSION: OnceCell<i32> = OnceCell::new();

/// Max non-default-database scrape queries that may run concurrently across the exporter,
/// set once at startup via CLI/env. Falls back to `MAX_DB_QUERY_CONCURRENCY`.
static MAX_DB_CONCURRENCY: OnceCell<usize> = OnceCell::new();

/// Global semaphore enforcing `MAX_DB_CONCURRENCY` across all multi-database collectors.
static DB_QUERY_SEMAPHORE: OnceCell<Arc<Semaphore>> = OnceCell::new();

/// Server-side `lock_timeout`, in milliseconds, set once at startup via CLI/env.
static LOCK_TIMEOUT_MS: OnceCell<u64> = OnceCell::new();

/// Server-side `statement_timeout`, in milliseconds, set once at startup via CLI/env.
static STATEMENT_TIMEOUT_MS: OnceCell<u64> = OnceCell::new();

/// Whole `/metrics` scrape timeout, in milliseconds, set once at startup via CLI/env.
static SCRAPE_TIMEOUT_MS: OnceCell<u64> = OnceCell::new();

/// Client-side connect timeout, in milliseconds, set once at startup via CLI/env.
static CONNECT_TIMEOUT_MS: OnceCell<u64> = OnceCell::new();

/// Common constants for `PostgreSQL` system schemas
pub const PG_CATALOG: &str = "pg_catalog";
pub const INFORMATION_SCHEMA: &str = "information_schema";

/// Common constants for `PostgreSQL` template databases
pub const TEMPLATE0: &str = "template0";
pub const TEMPLATE1: &str = "template1";

/// Time conversion factors
pub const MS_TO_SEC: f64 = 1000.0;

const DEFAULT_APPLICATION_NAME: &str = env!("CARGO_PKG_NAME");

/// A permit proving a non-default-database scrape query has been admitted by the global
/// concurrency limiter.
pub type DbQueryPermit = OwnedSemaphorePermit;

#[inline]
#[must_use]
pub fn apply_default_application_name(opts: PgConnectOptions) -> PgConnectOptions {
    opts.application_name(DEFAULT_APPLICATION_NAME)
}

/// Apply the shared hardening every scrape connection needs: the default application name,
/// a server-side `lock_timeout`, and a server-side `statement_timeout`.
///
/// The timeout is set as a connection startup option (`-c lock_timeout=...`) so it is
/// enforced by the backend itself. This is what prevents blocked scrape backends from
/// accumulating: a client-side (tokio) timeout only drops the client future — a backend
/// already waiting on a lock stays blocked server-side, holding its connection slot, until
/// the lock is granted. A server-side `lock_timeout` aborts the waiting backend and frees
/// the slot instead. Without it, a single long-held `AccessExclusiveLock` (routine DDL such
/// as `ALTER TABLE`, `VACUUM FULL`, `REINDEX`, `TRUNCATE`, or an abandoned transaction) can
/// let blocked scrapes accumulate until `max_connections` is exhausted and the whole cluster
/// stops accepting connections.
///
/// `lock_timeout` is intentionally overridable via the DSN, including `lock_timeout=0`, so
/// operators can choose to disable only lock-wait aborts when they explicitly accept that
/// tradeoff. `statement_timeout` is different: it must stay positive so an already-running
/// backend query cannot outlive the scrape indefinitely, and it must be lower than the
/// whole-scrape timeout so `PostgreSQL` gives up before the HTTP scrape does. If the DSN sets
/// `statement_timeout=0` or a timeout that is not lower than the scrape timeout, startup
/// fails.
///
/// # Errors
///
/// Returns an error when the DSN disables `statement_timeout`.
pub fn apply_connection_hardening(opts: PgConnectOptions) -> Result<PgConnectOptions> {
    let mut opts = apply_default_application_name(opts);
    let existing_options = opts.get_options().map(str::to_string);

    if existing_options
        .as_deref()
        .is_none_or(|options| pg_option_value(options, "lock_timeout").is_none())
    {
        opts = opts.options([("lock_timeout", get_lock_timeout_ms().to_string())]);
    }

    match existing_options
        .as_deref()
        .and_then(|options| pg_option_value(options, "statement_timeout"))
    {
        Some(value) if is_disabled_timeout_value(&value) => Err(disabled_statement_timeout_error()),
        Some(value) => {
            validate_statement_timeout_value(&value)?;
            Ok(opts)
        }
        None => {
            let statement_timeout_ms = get_statement_timeout_ms();
            validate_statement_timeout_micros(
                u128::from(statement_timeout_ms) * 1_000,
                &format!("{statement_timeout_ms}ms"),
            )?;
            Ok(opts.options([("statement_timeout", statement_timeout_ms.to_string())]))
        }
    }
}

fn pg_option_value(options: &str, key: &str) -> Option<String> {
    let tokens = split_pg_options(options);
    let mut value = None;
    let mut index = 0;

    while let Some(token) = tokens.get(index) {
        if token == "-c" {
            if let Some(setting) = tokens.get(index + 1) {
                value = option_value_from_setting(setting, key).or(value);
            }
            index += 2;
            continue;
        }

        if let Some(setting) = token.strip_prefix("-c")
            && !setting.is_empty()
        {
            value = option_value_from_setting(setting, key).or(value);
            index += 1;
            continue;
        }

        if let Some(setting) = token.strip_prefix("--") {
            value = option_value_from_setting(setting, key).or(value);
            index += 1;
            continue;
        }

        value = option_value_from_setting(token, key).or(value);
        index += 1;
    }

    value
}

fn option_value_from_setting(setting: &str, key: &str) -> Option<String> {
    let (name, value) = setting.split_once('=')?;
    if name.trim() == key {
        Some(value.trim().to_string())
    } else {
        None
    }
}

fn split_pg_options(options: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut escaped = false;

    for ch in options.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        if ch == '\\' {
            escaped = true;
            continue;
        }

        if ch.is_whitespace() {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
        } else {
            current.push(ch);
        }
    }

    if escaped {
        current.push('\\');
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn is_disabled_timeout_value(value: &str) -> bool {
    timeout_value_micros(value).is_some_and(|micros| micros == 0)
}

fn disabled_statement_timeout_error() -> anyhow::Error {
    anyhow!(
        "statement_timeout=0 disables the server-side query timeout; set a positive statement_timeout or omit it to use the pg_exporter default"
    )
}

fn validate_statement_timeout_value(value: &str) -> Result<()> {
    let micros = timeout_value_micros(value).ok_or_else(|| {
        anyhow!(
            "statement_timeout value {value:?} is not a supported positive duration; use ms, s, min, h, or d units and keep it lower than scrape_timeout"
        )
    })?;
    validate_statement_timeout_micros(micros, value)
}

fn validate_statement_timeout_micros(timeout_micros: u128, value: &str) -> Result<()> {
    if timeout_micros == 0 {
        return Err(disabled_statement_timeout_error());
    }

    let scrape_timeout_micros = get_scrape_timeout().as_micros();
    if timeout_micros >= scrape_timeout_micros {
        return Err(anyhow!(
            "statement_timeout ({value}) must be lower than scrape_timeout ({:?}); increase --scrape.timeout-ms/PG_EXPORTER_SCRAPE_TIMEOUT_MS or lower statement_timeout",
            get_scrape_timeout()
        ));
    }

    Ok(())
}

fn timeout_value_micros(value: &str) -> Option<u128> {
    let normalized = value
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .trim()
        .to_ascii_lowercase();

    let (number, unit) = split_timeout_number_and_unit(&normalized)?;
    let multiplier = timeout_unit_micros(unit)?;
    decimal_scaled_to_micros(number, multiplier)
}

fn split_timeout_number_and_unit(value: &str) -> Option<(&str, &str)> {
    let mut end = 0;
    let mut saw_digit = false;
    let mut saw_dot = false;

    for (index, ch) in value.char_indices() {
        if index == 0 && ch == '+' {
            end = ch.len_utf8();
            continue;
        }

        if ch.is_ascii_digit() {
            saw_digit = true;
            end = index + ch.len_utf8();
            continue;
        }

        if ch == '.' && !saw_dot {
            saw_dot = true;
            end = index + ch.len_utf8();
            continue;
        }

        break;
    }

    if !saw_digit {
        return None;
    }

    let number = value.get(..end)?.trim();
    let unit = value.get(end..)?.trim();
    Some((number, unit))
}

fn timeout_unit_micros(unit: &str) -> Option<u128> {
    match unit {
        "" | "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" => Some(1_000),
        "us" | "usec" | "usecs" | "microsecond" | "microseconds" => Some(1),
        "s" | "sec" | "secs" | "second" | "seconds" => Some(1_000_000),
        "min" | "mins" | "minute" | "minutes" => Some(60_000_000),
        "h" | "hr" | "hrs" | "hour" | "hours" => Some(3_600_000_000),
        "d" | "day" | "days" => Some(86_400_000_000),
        _ => None,
    }
}

fn decimal_scaled_to_micros(number: &str, multiplier: u128) -> Option<u128> {
    let number = number.strip_prefix('+').unwrap_or(number);
    if number.is_empty() {
        return None;
    }

    let (whole, fractional) = number.split_once('.').unwrap_or((number, ""));
    let whole_value = if whole.is_empty() {
        0
    } else {
        parse_ascii_decimal_u128(whole)?
    };

    let mut micros = whole_value.checked_mul(multiplier)?;

    if !fractional.is_empty() {
        let fractional_value = parse_ascii_decimal_u128(fractional)?;
        let mut scale = 1_u128;
        for _ in fractional.bytes() {
            scale = scale.checked_mul(10)?;
        }
        micros = micros.checked_add(fractional_value.checked_mul(multiplier)? / scale)?;
    }

    Some(micros)
}

fn parse_ascii_decimal_u128(value: &str) -> Option<u128> {
    let mut parsed = 0_u128;
    for byte in value.bytes() {
        if !byte.is_ascii_digit() {
            return None;
        }
        parsed = parsed
            .checked_mul(10)?
            .checked_add(u128::from(byte - b'0'))?;
    }
    Some(parsed)
}

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
#[must_use]
pub fn is_database_excluded(datname: &str) -> bool {
    get_excluded_databases().iter().any(|d| d == datname)
}

/// Set the `PostgreSQL` version. Call this once during startup after connecting.
pub fn set_pg_version(version: i32) {
    let _ = PG_VERSION.set(version);
}

/// Get the `PostgreSQL` version number.
/// Returns 0 if not set (should never happen in production).
#[inline]
pub fn get_pg_version() -> i32 {
    PG_VERSION.get().copied().unwrap_or(0)
}

/// Check if `PostgreSQL` version is at least the specified minimum.
#[inline]
#[must_use]
pub fn is_pg_version_at_least(min_version: i32) -> bool {
    get_pg_version() >= min_version
}

/// Set the max per-database collection concurrency. Call this once at startup from
/// CLI/env. Values are clamped to at least 1 (a zero limit would deadlock collectors).
pub fn set_max_db_concurrency(value: usize) {
    let _ = MAX_DB_CONCURRENCY.set(sanitized_concurrency(value));
}

/// Set scrape timeout knobs from CLI/env. Call once during startup.
pub fn set_scrape_timeouts(
    connect_timeout_ms: u64,
    lock_timeout_ms: u64,
    statement_timeout_ms: u64,
    scrape_timeout_ms: u64,
) {
    let _ = CONNECT_TIMEOUT_MS.set(nonzero_timeout_or_default(
        connect_timeout_ms,
        super::DEFAULT_CONNECT_TIMEOUT_MS,
    ));
    let _ = LOCK_TIMEOUT_MS.set(nonzero_timeout_or_default(
        lock_timeout_ms,
        super::DEFAULT_LOCK_TIMEOUT_MS,
    ));
    let _ = STATEMENT_TIMEOUT_MS.set(nonzero_timeout_or_default(
        statement_timeout_ms,
        super::DEFAULT_STATEMENT_TIMEOUT_MS,
    ));
    let _ = SCRAPE_TIMEOUT_MS.set(nonzero_timeout_or_default(
        scrape_timeout_ms,
        super::DEFAULT_SCRAPE_TIMEOUT_MS,
    ));
}

/// Clamp a requested concurrency to a usable value: never zero (a zero-permit semaphore
/// would deadlock every multi-database collector).
#[inline]
#[must_use]
const fn sanitized_concurrency(value: usize) -> usize {
    if value == 0 { 1 } else { value }
}

#[inline]
#[must_use]
const fn nonzero_timeout_or_default(value: u64, default: u64) -> u64 {
    if value == 0 { default } else { value }
}

/// Get the max per-database collection concurrency, falling back to the compile-time
/// default (`MAX_DB_QUERY_CONCURRENCY`) when not explicitly set (e.g. in tests).
#[inline]
#[must_use]
pub fn get_max_db_concurrency() -> usize {
    MAX_DB_CONCURRENCY
        .get()
        .copied()
        .unwrap_or(super::MAX_DB_QUERY_CONCURRENCY)
}

#[inline]
#[must_use]
pub fn get_lock_timeout_ms() -> u64 {
    LOCK_TIMEOUT_MS
        .get()
        .copied()
        .unwrap_or(super::DEFAULT_LOCK_TIMEOUT_MS)
}

#[inline]
#[must_use]
pub fn get_connect_timeout() -> Duration {
    Duration::from_millis(
        CONNECT_TIMEOUT_MS
            .get()
            .copied()
            .unwrap_or(super::DEFAULT_CONNECT_TIMEOUT_MS),
    )
}

#[inline]
#[must_use]
pub fn get_statement_timeout_ms() -> u64 {
    STATEMENT_TIMEOUT_MS
        .get()
        .copied()
        .unwrap_or(super::DEFAULT_STATEMENT_TIMEOUT_MS)
}

#[inline]
#[must_use]
pub fn get_scrape_timeout() -> Duration {
    Duration::from_millis(
        SCRAPE_TIMEOUT_MS
            .get()
            .copied()
            .unwrap_or(super::DEFAULT_SCRAPE_TIMEOUT_MS),
    )
}

/// Validate that connection attempts fail before the whole HTTP scrape does.
///
/// This keeps a plain database connectivity outage in the normal `200` + `pg_up 0` path
/// instead of letting the outer `/metrics` timeout fire first.
///
/// # Errors
///
/// Returns an error when the connect timeout is not lower than the scrape timeout.
pub fn validate_connect_timeout_budget() -> Result<()> {
    validate_connect_timeout_budget_for(get_connect_timeout(), get_scrape_timeout())
}

fn validate_connect_timeout_budget_for(
    connect_timeout: Duration,
    scrape_timeout: Duration,
) -> Result<()> {
    if connect_timeout >= scrape_timeout {
        return Err(anyhow!(
            "connect_timeout ({connect_timeout:?}) must be lower than scrape_timeout ({scrape_timeout:?}); increase --scrape.timeout-ms/PG_EXPORTER_SCRAPE_TIMEOUT_MS or lower --scrape.connect-timeout-ms/PG_EXPORTER_CONNECT_TIMEOUT_MS"
        ));
    }

    Ok(())
}

/// Acquire a permit from the global non-default-database query limiter.
///
/// # Errors
///
/// Returns an error if the limiter has been closed.
pub async fn acquire_db_query_permit() -> Result<DbQueryPermit> {
    DB_QUERY_SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(get_max_db_concurrency())))
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| anyhow!("database query concurrency semaphore closed"))
}

/// Initialize (idempotent) the base connect options from the provided DSN (`SecretString`).
/// Also records the default database name for default-pool routing.
///
/// # Errors
///
/// Returns an error if DSN parsing fails
pub fn set_base_connect_options_from_dsn(dsn: &SecretString) -> Result<()> {
    if BASE_OPTS.get().is_none() {
        let opts = apply_connection_hardening(PgConnectOptions::from_str(dsn.expose_secret())?)?;
        let _ = BASE_OPTS.set(opts.clone());

        // Record default database name if present, else fallback to "postgres".
        // PgConnectOptions::get_database() returns Option<&str>.
        let dbname = opts.get_database().unwrap_or("postgres").to_string();
        let _ = DEFAULT_DB.set(dbname);
    }

    Ok(())
}

/// Returns the default database name derived from the DSN, if available.
#[inline]
pub fn get_default_database() -> Option<&'static str> {
    DEFAULT_DB.get().map(std::string::String::as_str)
}

/// Build connect options for a specific database name based on the base DSN.
///
/// # Errors
///
/// Returns an error if base options are not initialized
pub fn connect_options_for_db(datname: &str) -> Result<PgConnectOptions> {
    let base = BASE_OPTS.get().cloned().ok_or_else(|| {
        anyhow!("BASE_OPTS not set; call set_base_connect_options_from_dsn() at startup")
    })?;
    Ok(base.database(datname))
}

/// Open a fresh connection to the specified non-default database.
///
/// Connections are intentionally **not** pooled or cached: the caller runs a single scrape
/// query and drops the connection, which closes it. Combined with the per-collector
/// concurrency limit, this bounds the exporter's per-database connection footprint to the
/// concurrency limit — regardless of how many databases exist — instead of pinning one
/// persistent connection per database (which would exhaust `max_connections` on large or
/// connection-constrained clusters, e.g. AWS RDS). The default database must use the shared
/// pool created at startup.
///
/// # Errors
///
/// Returns an error if called for the default database, or if the connection fails.
pub async fn open_db_connection(datname: &str, _permit: &DbQueryPermit) -> Result<PgConnection> {
    if let Some(def) = get_default_database()
        && def == datname
    {
        return Err(anyhow!(
            "open_db_connection called for default database; use shared pool"
        ));
    }

    let opts = connect_options_for_db(datname)?;
    let connect_timeout = get_connect_timeout();
    let conn = timeout(connect_timeout, PgConnection::connect_with(&opts))
        .await
        .map_err(|_| {
            anyhow!(
                "connecting to database {datname:?} exceeded connect timeout of {connect_timeout:?}"
            )
        })??;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_set_and_get_exclusions() {
        set_excluded_databases(vec![
            "postgres".into(),
            TEMPLATE0.into(),
            TEMPLATE0.into(), // duplicate
            " ".into(),       // empty after trim
        ]);

        let got = get_excluded_databases();
        assert_eq!(got, &["postgres".to_string(), TEMPLATE0.to_string()]);
        assert!(is_database_excluded("postgres"));
        assert!(!is_database_excluded("not_there"));
    }

    #[test]
    fn test_sanitized_concurrency_never_zero() {
        // Zero would create a zero-permit semaphore and deadlock the collectors.
        assert_eq!(sanitized_concurrency(0), 1);
        assert_eq!(sanitized_concurrency(1), 1);
        assert_eq!(sanitized_concurrency(5), 5);
        assert_eq!(sanitized_concurrency(4096), 4096);
        assert_eq!(sanitized_concurrency(usize::MAX), usize::MAX);
    }

    #[test]
    fn test_get_max_db_concurrency_defaults_to_const_and_is_nonzero() {
        // Without an explicit set, the getter returns the compile-time default, and it is
        // always usable (>= 1) regardless of whether startup wiring ran.
        let value = get_max_db_concurrency();
        assert!(value >= 1, "concurrency must never be zero, got {value}");
        // When unset it must equal the shared default constant.
        if super::MAX_DB_CONCURRENCY.get().is_none() {
            assert_eq!(value, crate::collectors::MAX_DB_QUERY_CONCURRENCY);
        }
    }

    #[test]
    fn test_get_connect_timeout_defaults_to_const_and_is_nonzero() {
        let value = get_connect_timeout();
        assert!(!value.is_zero(), "connect timeout must never be zero");

        if super::CONNECT_TIMEOUT_MS.get().is_none() {
            assert_eq!(
                value,
                Duration::from_millis(crate::collectors::DEFAULT_CONNECT_TIMEOUT_MS)
            );
        }
    }

    #[test]
    fn test_validate_connect_timeout_budget_accepts_default() -> Result<()> {
        validate_connect_timeout_budget()
    }

    #[test]
    fn test_validate_connect_timeout_budget_rejects_timeout_at_or_above_scrape() {
        for connect_timeout in [Duration::from_secs(15), Duration::from_secs(16)] {
            let error =
                validate_connect_timeout_budget_for(connect_timeout, Duration::from_secs(15))
                    .err()
                    .map(|error| error.to_string());

            assert!(
                error.is_some_and(|message| message.contains("must be lower than scrape_timeout")),
                "connect timeout {connect_timeout:?} must be rejected"
            );
        }
    }

    #[test]
    fn test_pg_version_utilities() {
        let initial = get_pg_version();
        if initial == 0 {
            assert!(!is_pg_version_at_least(140_000));

            set_pg_version(160_000); // PostgreSQL 16
            assert_eq!(get_pg_version(), 160_000);
            assert!(is_pg_version_at_least(140_000)); // >= 14
            assert!(is_pg_version_at_least(160_000)); // >= 16
            assert!(!is_pg_version_at_least(170_000)); // < 17
        } else {
            // Other DB-backed tests may initialize this OnceCell first when
            // PG_EXPORTER_DSN is present. Verify the helpers without depending on
            // test execution order.
            assert_eq!(get_pg_version(), initial);
            assert!(is_pg_version_at_least(initial));
            assert!(!is_pg_version_at_least(initial + 1));

            set_pg_version(160_000);
            assert_eq!(get_pg_version(), initial);
        }
    }

    #[test]
    fn test_apply_default_application_name_sets_pkg_name() -> Result<()> {
        let opts = PgConnectOptions::from_str("postgresql://localhost/postgres")?;
        let formatted = format!("{:?}", apply_default_application_name(opts));

        assert!(formatted.contains("application_name"));
        assert!(formatted.contains(DEFAULT_APPLICATION_NAME));
        Ok(())
    }

    #[test]
    fn test_apply_connection_hardening_sets_default_lock_timeout() -> Result<()> {
        // With no lock_timeout in the DSN, the safe default must be injected as a
        // server-side startup option so blocked scrape backends abort instead of piling up.
        let opts = PgConnectOptions::from_str("postgresql://localhost/postgres")?;
        let hardened = apply_connection_hardening(opts)?;
        let formatted = format!("{hardened:?}");
        let options = hardened.get_options().unwrap_or_default();

        assert!(formatted.contains(DEFAULT_APPLICATION_NAME));
        assert!(
            options.contains(&format!(
                "lock_timeout={}",
                crate::collectors::DEFAULT_LOCK_TIMEOUT_MS
            )),
            "expected default lock_timeout in {formatted}"
        );
        assert!(
            options.contains(&format!(
                "statement_timeout={}",
                crate::collectors::DEFAULT_STATEMENT_TIMEOUT_MS
            )),
            "expected default statement_timeout in {formatted}"
        );
        Ok(())
    }

    #[test]
    fn test_apply_connection_hardening_respects_dsn_lock_timeout() -> Result<()> {
        // An operator-provided lock_timeout (via the DSN `options` parameter) must win: the
        // default is not injected, so the DSN value is the only one present.
        let opts = PgConnectOptions::from_str(
            "postgresql://localhost/postgres?options=-c%20lock_timeout%3D5000",
        )?;
        let hardened = apply_connection_hardening(opts)?;

        let options = hardened.get_options().unwrap_or_default();
        assert!(
            options.contains("lock_timeout=5000"),
            "operator lock_timeout should be preserved, got {options}"
        );
        assert!(
            !options.contains(&format!(
                "lock_timeout={}",
                crate::collectors::DEFAULT_LOCK_TIMEOUT_MS
            )),
            "default lock_timeout must not be appended when the DSN sets one, got {options}"
        );
        Ok(())
    }

    #[test]
    fn test_apply_connection_hardening_allows_dsn_lock_timeout_zero() -> Result<()> {
        let opts = PgConnectOptions::from_str(
            "postgresql://localhost/postgres?options=-c%20lock_timeout%3D0",
        )?;
        let hardened = apply_connection_hardening(opts)?;
        let options = hardened.get_options().unwrap_or_default();

        assert!(
            options.contains("lock_timeout=0"),
            "operator lock_timeout=0 should be preserved, got {options}"
        );
        assert!(
            options.contains(&format!(
                "statement_timeout={}",
                crate::collectors::DEFAULT_STATEMENT_TIMEOUT_MS
            )),
            "statement_timeout backstop must remain, got {options}"
        );
        Ok(())
    }

    #[test]
    fn test_apply_connection_hardening_does_not_confuse_deadlock_timeout() -> Result<()> {
        let opts = PgConnectOptions::from_str(
            "postgresql://localhost/postgres?options=-c%20deadlock_timeout%3D5000",
        )?;
        let hardened = apply_connection_hardening(opts)?;
        let options = hardened.get_options().unwrap_or_default();

        assert!(
            options.contains("deadlock_timeout=5000"),
            "operator deadlock_timeout should be preserved, got {options}"
        );
        assert!(
            options.contains(&format!(
                "lock_timeout={}",
                crate::collectors::DEFAULT_LOCK_TIMEOUT_MS
            )),
            "deadlock_timeout must not suppress the default lock_timeout, got {options}"
        );
        Ok(())
    }

    #[test]
    fn test_apply_connection_hardening_respects_positive_dsn_statement_timeout() -> Result<()> {
        let opts = PgConnectOptions::from_str(
            "postgresql://localhost/postgres?options=-c%20statement_timeout%3D5000",
        )?;
        let hardened = apply_connection_hardening(opts)?;
        let options = hardened.get_options().unwrap_or_default();

        assert!(
            options.contains("statement_timeout=5000"),
            "operator statement_timeout should be preserved, got {options}"
        );
        assert!(
            !options.contains(&format!(
                "statement_timeout={}",
                crate::collectors::DEFAULT_STATEMENT_TIMEOUT_MS
            )),
            "default statement_timeout must not be appended when DSN sets one, got {options}"
        );
        Ok(())
    }

    #[test]
    fn test_apply_connection_hardening_rejects_disabled_statement_timeout() -> Result<()> {
        for value in [
            "0",
            "00",
            "+0",
            "0.0",
            "0ms",
            "0s",
            "0 seconds",
            "0 milliseconds",
        ] {
            let opts = PgConnectOptions::from_str(&format!(
                "postgresql://localhost/postgres?options=-c%20statement_timeout%3D{}",
                value.replace('+', "%2B").replace(' ', "%20")
            ))?;

            let Err(error) = apply_connection_hardening(opts) else {
                return Err(anyhow!("statement_timeout=0 must be rejected"));
            };
            assert!(
                error.to_string().contains("statement_timeout=0"),
                "unexpected error for value {value:?}: {error}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_apply_connection_hardening_rejects_statement_timeout_not_below_scrape_timeout()
    -> Result<()> {
        for value in ["15000", "15s", "1min"] {
            let opts = PgConnectOptions::from_str(&format!(
                "postgresql://localhost/postgres?options=-c%20statement_timeout%3D{}",
                value.replace(' ', "%20")
            ))?;

            let Err(error) = apply_connection_hardening(opts) else {
                return Err(anyhow!(
                    "statement_timeout {value:?} must be rejected when it is not below scrape_timeout"
                ));
            };
            assert!(
                error
                    .to_string()
                    .contains("must be lower than scrape_timeout"),
                "unexpected error for value {value:?}: {error}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_apply_connection_hardening_rejects_unvalidated_statement_timeout() -> Result<()> {
        let opts = PgConnectOptions::from_str(
            "postgresql://localhost/postgres?options=-c%20statement_timeout%3Ddefault",
        )?;

        let Err(error) = apply_connection_hardening(opts) else {
            return Err(anyhow!("statement_timeout=default must be rejected"));
        };
        assert!(
            error
                .to_string()
                .contains("is not a supported positive duration"),
            "unexpected error: {error}"
        );
        Ok(())
    }
}
