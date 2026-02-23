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
            collectors,
        } => {
            new(port, listen, dsn, collectors).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::BoxFuture;
    use secrecy::SecretString;

    #[tokio::test]
    async fn test_handle_action_signature() {
        // Test that handle accepts a valid Action and returns Result<()>
        // Note: We don't call handle(action).await here because it starts a blocking
        // HTTP server which would hang the test suite.
        // Instead, we just verify the types and that it compiles.

        let _action = Action::Run {
            port: 9999,
            listen: None,
            dsn: SecretString::new("postgresql://localhost/test".into()),
            collectors: vec!["default".to_string()],
        };

        // Signature check: handle is an async function taking Action and returning Result<()>
        let _: fn(Action) -> BoxFuture<'static, Result<()>> = |a| Box::pin(handle(a));
    }

    #[test]
    fn test_action_creation() {
        // Test that we can create Action::Run with valid parameters
        let action = Action::Run {
            port: 9432,
            listen: Some("127.0.0.1".to_string()),
            dsn: SecretString::new("postgresql://user@host/db".into()),
            collectors: vec!["default".to_string(), "vacuum".to_string()],
        };

        match action {
            Action::Run {
                port,
                listen,
                dsn: _,
                collectors,
            } => {
                assert_eq!(port, 9432);
                assert_eq!(listen, Some("127.0.0.1".to_string()));
                assert_eq!(collectors.len(), 2);
                assert!(collectors.contains(&"default".to_string()));
                assert!(collectors.contains(&"vacuum".to_string()));
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
            collectors: vec![],
        };

        match action {
            Action::Run { collectors, .. } => {
                assert_eq!(collectors.len(), 0, "Should allow empty collectors list");
            }
        }
    }
}
