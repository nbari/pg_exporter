use crate::{
    cli::actions::Action,
    collectors::{
        COLLECTOR_NAMES, Collector, all_factories,
        util::{get_excluded_databases, set_excluded_databases},
    },
};
use anyhow::Result;
use clap::ArgMatches;
use secrecy::SecretString;
use tracing::info;

pub fn handler(matches: &clap::ArgMatches) -> Result<Action> {
    // Initialize global excluded database list once from CLI/env
    init_excluded_databases(matches);

    info!("Excluded databases: {:?}", get_excluded_databases());

    Ok(Action::Run {
        port: matches.get_one::<u16>("port").copied().unwrap_or(9432),
        dsn: SecretString::from(
            matches
                .get_one::<String>("dsn")
                .map(|s: &String| s.to_string())
                .ok_or_else(|| {
                    anyhow::anyhow!("DSN is required. Please provide it using the --dsn flag.")
                })?,
        ),
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
