use crate::cli::{actions::Action, commands, dispatch::handler, telemetry};
use anyhow::Result;

/// Map verbosity count to tracing level
const fn get_verbosity_level(verbose_count: u8) -> Option<tracing::Level> {
    match verbose_count {
        0 => None,
        1 => Some(tracing::Level::INFO),
        2 => Some(tracing::Level::DEBUG),
        _ => Some(tracing::Level::TRACE),
    }
}

/// Start the CLI
///
/// # Errors
///
/// Returns an error if telemetry initialization or command handling fails
pub fn start() -> Result<Action> {
    let matches = commands::new().get_matches();

    let verbosity_level = get_verbosity_level(matches.get_count("verbose"));

    telemetry::init(verbosity_level)?;

    let action = handler(&matches)?;

    Ok(action)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_verbosity_level_none() {
        assert_eq!(get_verbosity_level(0), None);
    }

    #[test]
    fn test_get_verbosity_level_info() {
        assert_eq!(get_verbosity_level(1), Some(tracing::Level::INFO));
    }

    #[test]
    fn test_get_verbosity_level_debug() {
        assert_eq!(get_verbosity_level(2), Some(tracing::Level::DEBUG));
    }

    #[test]
    fn test_get_verbosity_level_trace() {
        assert_eq!(get_verbosity_level(3), Some(tracing::Level::TRACE));
    }

    #[test]
    fn test_get_verbosity_level_max() {
        // Any value > 3 should return TRACE
        assert_eq!(get_verbosity_level(4), Some(tracing::Level::TRACE));
        assert_eq!(get_verbosity_level(10), Some(tracing::Level::TRACE));
        assert_eq!(get_verbosity_level(255), Some(tracing::Level::TRACE));
    }
}
