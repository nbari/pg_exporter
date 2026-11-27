//! Tests of the test fixtures themselves. They need no database.

use super::common;
use anyhow::Result;
use sqlx::postgres::PgConnectOptions;
use std::str::FromStr;

/// An "isolated" database is only isolated if the derived DSN really selects it. `SQLx`
/// lets a `dbname` query parameter override the URL path, so a base DSN carrying one
/// used to keep the fixture in the caller's database: a test that dropped and recreated
/// an extension did it there, and passed.
#[test]
fn test_derived_dsn_selects_the_isolated_database() -> Result<()> {
    for base in [
        "postgresql://postgres:postgres@localhost:5432/postgres",
        "postgresql://postgres:postgres@localhost:5432/postgres?dbname=caller_db",
        "postgresql://postgres:postgres@localhost:5432/postgres?sslmode=disable&dbname=caller_db",
        "postgresql://postgres:postgres@localhost:5432/?dbname=caller_db&application_name=x",
    ] {
        let derived = common::dsn_for_database(base, "test_isolated_1")?;
        let options = PgConnectOptions::from_str(&derived)?;
        assert_eq!(
            options.get_database(),
            Some("test_isolated_1"),
            "{base} should derive a DSN for the isolated database, got {derived}"
        );
    }

    let kept = common::dsn_for_database(
        "postgresql://postgres@localhost/postgres?sslmode=disable&dbname=caller_db",
        "test_isolated_1",
    )?;
    assert!(
        kept.contains("sslmode=disable"),
        "unrelated parameters must survive, got {kept}"
    );
    Ok(())
}

/// The same for the role: `user` and `password` in the query string override the URL's
/// user info, which would log the "unprivileged" connection in as the caller.
#[test]
fn test_derived_dsn_selects_the_requested_role() -> Result<()> {
    for base in [
        "postgresql://postgres:postgres@localhost:5432/postgres",
        "postgresql://postgres:postgres@localhost:5432/postgres?user=postgres&password=postgres",
        "postgresql://localhost:5432/postgres?user=postgres&dbname=caller_db",
    ] {
        let derived = common::dsn_for_database_role(base, "test_isolated_1", "probe", "secret")?;
        let options = PgConnectOptions::from_str(&derived)?;
        assert_eq!(
            options.get_username(),
            "probe",
            "{base} should derive a DSN for the probe role, got {derived}"
        );
        assert_eq!(options.get_database(), Some("test_isolated_1"));
    }
    Ok(())
}
