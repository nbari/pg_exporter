//! The one place a test learns which `PostgreSQL` it may use.
//!
//! The suite creates and drops databases, terminates backends and throttles vacuums, so
//! it must only ever run against a disposable local server. Every test, in the library
//! and in `tests/`, takes its DSN from [`test_dsn`], which refuses anything else.
//!
//! The check is on where a connection would really go, not on what the DSN looks like.
//! A `hostaddr` parameter, or `PGHOSTADDR` in the environment, makes `SQLx` connect to
//! that address whatever the host name says, and a DSN can contain the word `localhost`
//! in its database name or options while pointing at another machine.
// Included by the library's unit tests and by several test binaries; each uses a part.
#![allow(dead_code)]
#![allow(clippy::panic)]

use sqlx::postgres::PgConnectOptions;
use std::{env, net::IpAddr, str::FromStr};

/// DSN used when `PG_EXPORTER_DSN` is not set.
pub const DEFAULT_TEST_DSN: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

/// Returns the DSN tests connect to.
///
/// # Panics
///
/// Panics when the DSN would connect to another machine. A devcontainer whose
/// `PostgreSQL` is a trusted, disposable compose sibling reachable as `postgres` opts
/// out with `PG_EXPORTER_TEST_ALLOW_NONLOCAL=1` (set in `.devcontainer/compose.yaml`).
#[must_use]
pub fn test_dsn() -> String {
    // Some unit tests set `PG_EXPORTER_DSN` to a made-up value for a moment with
    // `temp_env`, in this same process. Reading under `temp_env`'s lock sees the real
    // environment, not that value.
    let no_changes: [(&str, Option<&str>); 0] = [];
    let (dsn, allow_nonlocal, env_hostaddr) = temp_env::with_vars(no_changes, || {
        (
            env::var("PG_EXPORTER_DSN").unwrap_or_else(|_| DEFAULT_TEST_DSN.to_owned()),
            env::var("PG_EXPORTER_TEST_ALLOW_NONLOCAL")
                .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE")),
            env::var("PGHOSTADDR").ok(),
        )
    });

    if !allow_nonlocal && !target_is_local(&dsn, env_hostaddr.as_deref()) {
        eprintln!("WARNING: the test database is not on this machine!");
        eprintln!("DSN: {}", dsn.replace(char::is_alphanumeric, "*"));
        if env_hostaddr.is_some() {
            eprintln!("PGHOSTADDR is set and overrides the host name in the DSN.");
        }
        eprintln!("Tests should run against localhost only.");
        eprintln!("Use: just test (handles this automatically)");
        eprintln!("Or:   PG_EXPORTER_DSN='{DEFAULT_TEST_DSN}' cargo test");
        eprintln!(
            "(In a devcontainer with a trusted local postgres service, set PG_EXPORTER_TEST_ALLOW_NONLOCAL=1.)"
        );
        panic!("Refusing to run tests against a remote database. Use localhost.");
    }

    dsn
}

/// Whether a connection made from `dsn` would stay on this machine.
///
/// `env_hostaddr` is the value of `PGHOSTADDR`. `SQLx` connects to a `hostaddr` when
/// there is one, from the DSN first and the environment otherwise, and ignores the host
/// name for the connection. A DSN that cannot be parsed is not local.
#[must_use]
pub fn target_is_local(dsn: &str, env_hostaddr: Option<&str>) -> bool {
    let Ok(options) = PgConnectOptions::from_str(dsn) else {
        return false;
    };
    let host = options.get_host();
    if options.get_socket().is_some() || host.starts_with('/') {
        return true;
    }

    // The last `hostaddr` wins when the parameter is repeated, as it does in `SQLx`.
    let dsn_hostaddr = url::Url::parse(dsn).ok().and_then(|url| {
        url.query_pairs()
            .filter(|(key, _)| key == "hostaddr")
            .last()
            .map(|(_, value)| value.into_owned())
    });
    if let Some(address) = dsn_hostaddr.as_deref().or(env_hostaddr) {
        return address
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    }

    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}
