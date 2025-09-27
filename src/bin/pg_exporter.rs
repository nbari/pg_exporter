use anyhow::Result;
use pg_exporter::cli::{actions, actions::Action, start};

// Main function
#[tokio::main]
async fn main() -> Result<()> {
    // Start the program
    let action = start()?;

    match action {
        Action::Run { .. } => actions::run::handle(action).await?,
    }

    Ok(())
}
