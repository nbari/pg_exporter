pub mod run;

use secrecy::SecretString;

#[derive(Debug)]
pub enum Action {
    Run { dsn: SecretString },
}
