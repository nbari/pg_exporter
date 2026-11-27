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

/// The one host name `PG_EXPORTER_TEST_ALLOW_NONLOCAL=1` admits besides this machine: the
/// `PostgreSQL` service of the devcontainer's compose project.
pub const COMPOSE_SERVICE_HOST: &str = "postgres";

/// Returns the DSN tests connect to.
///
/// # Panics
///
/// Panics when the DSN would connect to another machine. A devcontainer whose
/// `PostgreSQL` is a trusted, disposable compose sibling opts in to that one service with
/// `PG_EXPORTER_TEST_ALLOW_NONLOCAL=1` (set in `.devcontainer/compose.yaml`); the variable
/// admits the host name `postgres` and nothing else.
#[must_use]
pub fn test_dsn() -> String {
    // Some unit tests set `PG_EXPORTER_DSN` to a made-up value for a moment with
    // `temp_env`, in this same process. Reading under `temp_env`'s lock sees the real
    // environment, not that value.
    let no_changes: [(&str, Option<&str>); 0] = [];
    let (dsn, hostaddr_is_set) = temp_env::with_vars(no_changes, || {
        (
            env::var("PG_EXPORTER_DSN").unwrap_or_else(|_| DEFAULT_TEST_DSN.to_owned()),
            env::var("PGHOSTADDR").is_ok(),
        )
    });

    if !target_is_allowed(&dsn) {
        eprintln!("WARNING: the test database is not on this machine!");
        eprintln!("DSN: {}", dsn.replace(char::is_alphanumeric, "*"));
        if hostaddr_is_set {
            eprintln!("PGHOSTADDR is set and overrides the host name in the DSN.");
        }
        eprintln!("Tests should run against localhost only.");
        eprintln!("Use: just test (handles this automatically)");
        eprintln!("Or:   PG_EXPORTER_DSN='{DEFAULT_TEST_DSN}' cargo test");
        eprintln!(
            "(In a devcontainer, PG_EXPORTER_TEST_ALLOW_NONLOCAL=1 admits the compose service host '{COMPOSE_SERVICE_HOST}' only.)"
        );
        panic!("Refusing to run tests against a remote database. Use localhost.");
    }

    dsn
}

/// Whether the suite may connect to `dsn`: this machine, or, with the devcontainer
/// opt-in, its compose service. This is the rule [`test_dsn`] applies to
/// `PG_EXPORTER_DSN`; a test that derives another DSN from it (another role, other
/// options) checks the result with this before it connects, because a `hostaddr` kept by
/// the original and lost in the copy lets `PGHOSTADDR` decide again.
#[must_use]
pub fn target_is_allowed(dsn: &str) -> bool {
    let no_changes: [(&str, Option<&str>); 0] = [];
    let (allow_nonlocal, env_hostaddr) = temp_env::with_vars(no_changes, || {
        (
            env::var("PG_EXPORTER_TEST_ALLOW_NONLOCAL")
                .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE")),
            env::var("PGHOSTADDR").ok(),
        )
    });
    target_is_local(dsn, env_hostaddr.as_deref())
        || (allow_nonlocal && target_is_compose_service(dsn, env_hostaddr.as_deref()))
}

/// Refuses, by panicking, a DSN a test built itself (for a container it started) when a
/// connection made from it would not stay on this machine.
///
/// Such a DSN names `127.0.0.1` and a mapped port, but `PGHOSTADDR` in the environment
/// still redirects it: `SQLx` would knock on that port of another machine.
///
/// # Panics
///
/// Panics when the effective target is not local.
pub fn ensure_local(dsn: &str) {
    let no_changes: [(&str, Option<&str>); 0] = [];
    let env_hostaddr = temp_env::with_vars(no_changes, || env::var("PGHOSTADDR").ok());
    assert!(
        target_is_local(dsn, env_hostaddr.as_deref()),
        "Refusing to connect a test to another machine: the DSN for a local container does \
         not resolve locally (is PGHOSTADDR set?)"
    );
}

/// Whether `dsn` names the devcontainer's compose service by host name, with no
/// `hostaddr` anywhere to send the connection somewhere else.
#[must_use]
pub fn target_is_compose_service(dsn: &str, env_hostaddr: Option<&str>) -> bool {
    let Ok(options) = PgConnectOptions::from_str(dsn) else {
        return false;
    };
    let dsn_has_hostaddr =
        url::Url::parse(dsn).is_ok_and(|url| url.query_pairs().any(|(key, _)| key == "hostaddr"));
    options.get_socket().is_none()
        && options.get_host() == COMPOSE_SERVICE_HOST
        && !dsn_has_hostaddr
        && env_hostaddr.is_none()
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
