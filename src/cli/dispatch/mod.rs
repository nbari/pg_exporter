use crate::cli::actions::Action;
use anyhow::Result;
use secrecy::SecretString;

pub fn handler(matches: &clap::ArgMatches) -> Result<Action> {
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
    })
}
