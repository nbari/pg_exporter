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
use tracing::info;

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
    let listen = matches.get_one::<String>("listen").map(|s| s.to_string());

    // Get the DSN or return an error
    let dsn = SecretString::from(
        matches
            .get_one::<String>("dsn")
            .map(|s: &String| s.to_string())
            .ok_or_else(|| anyhow!("DSN is required. Please provide it using the --dsn flag."))?,
    );

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

pub fn get_enabled_collectors(matches: &ArgMatches) -> Vec<String> {
    let factories = all_factories();

    COLLECTOR_NAMES
        .iter()
        .filter(|&name| {
            let enable_flag = format!("collector.{}", name);
            let disable_flag = format!("no-collector.{}", name);

            // If explicitly disabled, skip it
            if matches.get_flag(&disable_flag) {
                return false;
            }

            // If explicitly enabled, include it
            if matches.get_flag(&enable_flag) {
                return true;
            }

            // Otherwise, check the collector's default setting
            if let Some(factory) = factories.get(name) {
                let collector = factory();
                collector.enabled_by_default()
            } else {
                false // Fallback if collector not found
            }
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
}
