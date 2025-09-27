pub mod run;

use secrecy::SecretString;

#[derive(Debug)]
pub enum Action {
    Run { port: u16, dsn: SecretString },
}
