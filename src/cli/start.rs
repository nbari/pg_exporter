use crate::cli::{actions::Action, commands, dispatch::handler, telemetry};
use anyhow::Result;

/// Start the CLI
pub fn start() -> Result<Action> {
    let matches = commands::new().get_matches();

    let verbosity_level = match matches.get_count("verbose") {
        0 => None,
        1 => Some(tracing::Level::INFO),
        2 => Some(tracing::Level::DEBUG),
        3 => Some(tracing::Level::TRACE),
        _ => Some(tracing::Level::TRACE),
    };

    telemetry::init(verbosity_level)?;

    let action = handler(&matches)?;

    Ok(action)
}
