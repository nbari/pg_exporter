pub mod run;

use crate::collectors::config::CollectorConfig;
use secrecy::SecretString;

#[derive(Debug)]
pub enum Action {
    Run {
        port: u16,
        listen: Option<String>,
        dsn: SecretString,
        collector_config: CollectorConfig,
    },
}
