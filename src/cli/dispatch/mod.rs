use crate::cli::actions::Action;
use anyhow::Result;
use secrecy::SecretString;

pub fn handler(matches: &clap::ArgMatches) -> Result<Action> {
    Ok(Action::Run {
        dsn: SecretString::from(
            matches
                .get_one::<String>("dsn")
                .map(|s| s.to_string())
                .unwrap_or_default(),
        ),
    })
}
