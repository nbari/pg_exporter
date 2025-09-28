use crate::cli::actions::Action;
use crate::exporter::new;
use anyhow::Result;

/// Handle the create action
pub async fn handle(action: Action) -> Result<()> {
    match action {
        Action::Run {
            port,
            dsn,
            collectors,
        } => {
            new(port, dsn, collectors).await?;
        }
    }

    Ok(())
}
