use crate::cli::actions::Action;
use anyhow::Result;

/// Handle the create action
pub async fn handle(action: Action) -> Result<()> {
    match action {
        Action::Run { dsn } => {
            todo!("Implement the run action with dsn: {:?}", dsn);
        }
    }
}
