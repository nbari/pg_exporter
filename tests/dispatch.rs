use anyhow::Result;
use secrecy::ExposeSecret;

use pg_exporter::{
    cli::{actions::Action, commands},
    collectors::util::get_excluded_databases,
};

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
    let excluded = get_excluded_databases();
    let v: Vec<String> = excluded.to_vec();

    println!("Excluded databases: {:?}", v);

    assert!(v.contains(&"db1".to_string()));
    assert!(v.contains(&"db2".to_string()));
    assert!(v.contains(&"db3".to_string()));

    Ok(())
}
