use crate::{
    cli::actions::Action,
    collectors::{
        COLLECTOR_NAMES, Collector, all_factories,
        util::{get_excluded_databases, set_excluded_databases},
    },
};
use anyhow::{Result, anyhow};
use clap::ArgMatches;
use secrecy::SecretString;
use std::fs;
use tracing::info;

/// Read DSN with priority: `PG_EXPORTER_DSN_FILE` > `PG_EXPORTER_DSN`/--dsn > default
///
/// # Errors
///
/// Returns an error if file cannot be read or DSN is missing
fn get_dsn(matches: &ArgMatches) -> Result<String> {
    // Priority 1: Check PG_EXPORTER_DSN_FILE
    if let Ok(file_path) = std::env::var("PG_EXPORTER_DSN_FILE") {
        let contents = fs::read_to_string(&file_path)
            .map_err(|e| anyhow!("Failed to read DSN from file '{file_path}': {e}"))?;
        return Ok(contents.trim().to_string());
    }

    // Priority 2: Use clap value (PG_EXPORTER_DSN env or --dsn flag)
    matches
        .get_one::<String>("dsn")
        .cloned()
        .ok_or_else(|| anyhow!("DSN is required. Please provide it using the --dsn flag."))
}

/// # Errors
///
/// Returns an error if required arguments are missing or collector validation fails
pub fn handler(matches: &clap::ArgMatches) -> Result<Action> {
    // Initialize global excluded database list once from CLI/env
    init_excluded_databases(matches);

    info!("Excluded databases: {:?}", get_excluded_databases());

    // Get the port or return an error
    let port = matches
        .get_one::<u16>("port")
        .copied()
        .ok_or_else(|| anyhow!("Port is required. Please provide it using the --port flag."))?;

    // Get the listen address (None means auto-detect)
    let listen = matches
        .get_one::<String>("listen")
        .map(std::string::ToString::to_string);

    // Get the DSN (checks PG_EXPORTER_DSN_FILE first, then env/flag)
    let dsn = SecretString::from(get_dsn(matches)?);

    Ok(Action::Run {
        port,
        listen,
        dsn,
        collectors: get_enabled_collectors(matches),
    })
}

fn init_excluded_databases(matches: &ArgMatches) {
    // Collect values from Clap (supports --exclude-databases a,b and env)
    let excludes: Vec<String> = matches
        .get_many::<String>("exclude-databases")
        .map(|vals| {
            vals.map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    // Set once globally for all collectors
    set_excluded_databases(excludes);
}

#[must_use]
pub fn get_enabled_collectors(matches: &ArgMatches) -> Vec<String> {
    let factories = all_factories();

    COLLECTOR_NAMES
        .iter()
        .filter(|&name| {
            let enable_flag = format!("collector.{name}");
            let disable_flag = format!("no-collector.{name}");

            // If explicitly disabled, skip it
            if matches.get_flag(&disable_flag) {
                return false;
            }

            // If explicitly enabled, include it
            if matches.get_flag(&enable_flag) {
                return true;
            }

            // Otherwise, check the collector's default setting
            factories.get(name).is_some_and(|factory| {
                let collector = factory();
                collector.enabled_by_default()
            })
        })
        .map(|&name| name.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::commands;

    #[test]
    fn test_get_enabled_collectors_defaults() {
        let command = commands::new();
        let matches = command.get_matches_from(vec!["pg_exporter"]);
        let enabled = get_enabled_collectors(&matches);

        assert!(enabled.contains(&"default".to_string()));
        assert!(enabled.contains(&"activity".to_string()));
        assert!(enabled.contains(&"vacuum".to_string()));
    }

    #[test]
    fn test_get_enabled_collectors_explicit_enable() {
        let command = commands::new();
        let matches =
            command.get_matches_from(vec!["pg_exporter", "--collector.locks", "--collector.stat"]);
        let enabled = get_enabled_collectors(&matches);

        assert!(enabled.contains(&"locks".to_string()));
        assert!(enabled.contains(&"stat".to_string()));
        assert!(enabled.contains(&"default".to_string()));
    }

    #[test]
    fn test_get_enabled_collectors_explicit_disable() {
        let command = commands::new();
        let matches = command.get_matches_from(vec!["pg_exporter", "--no-collector.vacuum"]);
        let enabled = get_enabled_collectors(&matches);

        assert!(!enabled.contains(&"vacuum".to_string()));
        assert!(enabled.contains(&"default".to_string()));
        assert!(enabled.contains(&"activity".to_string()));
    }

    #[test]
    fn test_get_enabled_collectors_disable_all_defaults() {
        let command = commands::new();
        let matches = command.get_matches_from(vec![
            "pg_exporter",
            "--no-collector.default",
            "--no-collector.activity",
            "--no-collector.vacuum",
        ]);
        let enabled = get_enabled_collectors(&matches);

        assert!(!enabled.contains(&"default".to_string()));
        assert!(!enabled.contains(&"activity".to_string()));
        assert!(!enabled.contains(&"vacuum".to_string()));
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_get_dsn_from_env() {
        temp_env::with_var("PG_EXPORTER_DSN", Some("postgresql://test:5432/db"), || {
            temp_env::with_var("PG_EXPORTER_DSN_FILE", None::<String>, || {
                let command = commands::new();
                let matches = command.get_matches_from(vec!["pg_exporter"]);
                let dsn = get_dsn(&matches).unwrap();
                assert_eq!(dsn, "postgresql://test:5432/db");
            });
        });
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_get_dsn_from_file() {
        use std::io::Write;
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp_file.as_file(), "postgresql://file:5432/db").unwrap();

        temp_env::with_var(
            "PG_EXPORTER_DSN_FILE",
            Some(temp_file.path().to_str().unwrap()),
            || {
                temp_env::with_var("PG_EXPORTER_DSN", Some("postgresql://env:5432/db"), || {
                    let command = commands::new();
                    let matches = command.get_matches_from(vec!["pg_exporter"]);
                    let dsn = get_dsn(&matches).unwrap();
                    // DSN_FILE should take priority over DSN env
                    assert_eq!(dsn, "postgresql://file:5432/db");
                });
            },
        );
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_get_dsn_from_file_with_whitespace() {
        use std::io::Write;
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp_file.as_file(), "  postgresql://whitespace:5432/db  ").unwrap();

        temp_env::with_var(
            "PG_EXPORTER_DSN_FILE",
            Some(temp_file.path().to_str().unwrap()),
            || {
                let command = commands::new();
                let matches = command.get_matches_from(vec!["pg_exporter"]);
                let dsn = get_dsn(&matches).unwrap();
                // Should be trimmed
                assert_eq!(dsn, "postgresql://whitespace:5432/db");
            },
        );
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_get_dsn_file_not_found() {
        temp_env::with_var(
            "PG_EXPORTER_DSN_FILE",
            Some("/nonexistent/file.txt"),
            || {
                let command = commands::new();
                let matches = command.get_matches_from(vec!["pg_exporter"]);
                let result = get_dsn(&matches);
                assert!(result.is_err());
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("Failed to read DSN from file")
                );
            },
        );
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_get_dsn_cli_flag_does_not_override_file() {
        use std::io::Write;
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp_file.as_file(), "postgresql://file:5432/db").unwrap();

        temp_env::with_var(
            "PG_EXPORTER_DSN_FILE",
            Some(temp_file.path().to_str().unwrap()),
            || {
                let command = commands::new();
                let matches = command.get_matches_from(vec![
                    "pg_exporter",
                    "--dsn",
                    "postgresql://cli:5432/db",
                ]);
                let dsn = get_dsn(&matches).unwrap();
                // DSN_FILE takes highest priority
                assert_eq!(dsn, "postgresql://file:5432/db");
            },
        );
    }
}
