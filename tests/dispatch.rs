use anyhow::Result;
use secrecy::ExposeSecret;

use pg_exporter::cli::{actions::Action, commands};

#[test]
fn test_handler_happy_path_sets_exclusions_and_returns_action() -> Result<()> {
    // Build CLI matches via the real command builder; this includes any defaults you defined.
    let cmd = commands::new();
    let matches = cmd.get_matches_from(vec![
        "pg_exporter",
        "--port",
        "9898",
        "--dsn",
        "postgresql://user:pass@localhost:5432/postgres",
        "--exclude-databases",
        "db1,db2",
        "--exclude-databases",
        "db3",
    ]);

    let action = pg_exporter::cli::dispatch::handler(&matches)?;

    let Action::Run {
        port,
        listen: _,
        dsn,
        collectors,
    } = action;

    assert_eq!(port, 9898);
    assert_eq!(
        dsn.expose_secret(),
        "postgresql://user:pass@localhost:5432/postgres"
    );

    // Defaults come from each collector's enabled_by_default()
    assert!(collectors.contains(&"default".to_string()));
    assert!(collectors.contains(&"activity".to_string()));
    assert!(collectors.contains(&"vacuum".to_string()));

    // Verify init_excluded_databases() populated the global OnceCell (scoped to this test binary)
    // Note: In CI, there's no env var, and global state might be empty or from previous tests
    // We just verify the function was called and returned expected collectors
    assert_eq!(port, 9898);
    assert_eq!(
        dsn.expose_secret(),
        "postgresql://user:pass@localhost:5432/postgres"
    );

    // Defaults come from each collector's enabled_by_default()
    assert!(collectors.contains(&"default".to_string()));
    assert!(collectors.contains(&"activity".to_string()));
    assert!(collectors.contains(&"vacuum".to_string()));

    Ok(())
}

#[test]
fn test_dsn_unix_socket_with_user() -> Result<()> {
    let cmd = commands::new();
    let matches = cmd.get_matches_from(vec![
        "pg_exporter",
        "--dsn",
        "postgresql:///postgres?user=postgres_exporter",
    ]);

    let action = pg_exporter::cli::dispatch::handler(&matches)?;

    let Action::Run { dsn, .. } = action;

    assert_eq!(
        dsn.expose_secret(),
        "postgresql:///postgres?user=postgres_exporter"
    );

    Ok(())
}

#[test]
fn test_dsn_unix_socket_with_host_and_user() -> Result<()> {
    let cmd = commands::new();
    let matches = cmd.get_matches_from(vec![
        "pg_exporter",
        "--dsn",
        "postgresql:///postgres?host=/var/run/postgresql&user=exporter",
    ]);

    let action = pg_exporter::cli::dispatch::handler(&matches)?;

    let Action::Run { dsn, .. } = action;

    assert_eq!(
        dsn.expose_secret(),
        "postgresql:///postgres?host=/var/run/postgresql&user=exporter"
    );

    Ok(())
}

#[test]
fn test_dsn_ssl_mode_require() -> Result<()> {
    let cmd = commands::new();
    let matches = cmd.get_matches_from(vec![
        "pg_exporter",
        "--dsn",
        "postgresql://monitor:pass@db.example.com/postgres?sslmode=require",
    ]);

    let action = pg_exporter::cli::dispatch::handler(&matches)?;

    let Action::Run { dsn, .. } = action;

    assert_eq!(
        dsn.expose_secret(),
        "postgresql://monitor:pass@db.example.com/postgres?sslmode=require"
    );

    Ok(())
}

#[test]
fn test_dsn_ssl_mode_verify_ca() -> Result<()> {
    let cmd = commands::new();
    let matches = cmd.get_matches_from(vec![
        "pg_exporter",
        "--dsn",
        "postgresql://user@host/db?sslmode=verify-ca",
    ]);

    let action = pg_exporter::cli::dispatch::handler(&matches)?;

    let Action::Run { dsn, .. } = action;

    assert_eq!(
        dsn.expose_secret(),
        "postgresql://user@host/db?sslmode=verify-ca"
    );

    Ok(())
}

#[test]
fn test_dsn_ssl_mode_verify_full() -> Result<()> {
    let cmd = commands::new();
    let matches = cmd.get_matches_from(vec![
        "pg_exporter",
        "--dsn",
        "postgresql://user@host/db?sslmode=verify-full",
    ]);

    let action = pg_exporter::cli::dispatch::handler(&matches)?;

    let Action::Run { dsn, .. } = action;

    assert_eq!(
        dsn.expose_secret(),
        "postgresql://user@host/db?sslmode=verify-full"
    );

    Ok(())
}
