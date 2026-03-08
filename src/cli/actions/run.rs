use crate::cli::actions::Action;
use crate::exporter::new;
use anyhow::Result;

/// Handle the create action
///
/// # Errors
///
/// Returns an error if the exporter fails to start
pub async fn handle(action: Action) -> Result<()> {
    match action {
        Action::Run {
            port,
            listen,
            dsn,
            collector_config,
        } => {
            new(port, listen, dsn, collector_config).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collectors::config::CollectorConfig;
    use secrecy::SecretString;

    #[tokio::test]
    async fn test_handle_action_signature() {
        // Test that handle accepts a valid Action and returns Result<()>
        // We use an invalid DSN to ensure it returns an error instead of starting
        // the blocking HTTP server, allowing the test to complete.
        let action = Action::Run {
            port: 9999,
            listen: None,
            dsn: SecretString::new("invalid-dsn".into()),
            collector_config: CollectorConfig::new(25).with_enabled(&["default".to_string()]),
        };

        let result = handle(action).await;

        assert!(result.is_err(), "Should fail with invalid DSN");
    }

    #[test]
    fn test_action_creation() {
        // Test that we can create Action::Run with valid parameters
        let action = Action::Run {
            port: 9432,
            listen: Some("127.0.0.1".to_string()),
            dsn: SecretString::new("postgresql://user@host/db".into()),
            collector_config: CollectorConfig::new(25)
                .with_enabled(&["default".to_string(), "vacuum".to_string()]),
        };

        match action {
            Action::Run {
                port,
                listen,
                dsn: _,
                collector_config,
            } => {
                assert_eq!(port, 9432);
                assert_eq!(listen, Some("127.0.0.1".to_string()));
                assert!(collector_config.is_enabled("default"));
                assert!(collector_config.is_enabled("vacuum"));
            }
        }
    }

    #[test]
    fn test_action_with_empty_collectors() {
        // Test that Action can be created with empty collectors list
        let action = Action::Run {
            port: 8080,
            listen: None,
            dsn: SecretString::new("postgresql://localhost/db".into()),
            collector_config: CollectorConfig::new(25),
        };

        match action {
            Action::Run {
                collector_config, ..
            } => {
                assert!(
                    collector_config.enabled_collectors.is_empty(),
                    "Should allow empty collectors list"
                );
            }
        }
    }
}
