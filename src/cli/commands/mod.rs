use clap::{
    Arg, ArgAction, ColorChoice, Command,
    builder::styling::{AnsiColor, Effects, Styles},
};

pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub fn new() -> Command {
    let styles = Styles::styled()
        .header(AnsiColor::Yellow.on_default() | Effects::BOLD)
        .usage(AnsiColor::Green.on_default() | Effects::BOLD)
        .literal(AnsiColor::Blue.on_default() | Effects::BOLD)
        .placeholder(AnsiColor::Green.on_default());

    Command::new("pg_exporter")
        .about("PostgreSQL metric exporter for Prometheus")
        .version(env!("CARGO_PKG_VERSION"))
        .long_version(built_info::GIT_COMMIT_HASH.to_owned())
        .color(ColorChoice::Auto)
        .styles(styles)
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .help("Port to listen on")
                .default_value("9432")
                .env("PG_EXPORTER_PORT")
                .value_parser(clap::value_parser!(u16)),
        )
        .arg(
            Arg::new("dsn")
                .long("dsn")
                .help("Database connection string")
                .default_value("postgresql://postgresd@localhost:5432/postgres")
                .env("PG_EXPORTER_DSN")
                .value_name("DSN")
                .required(true),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Increase verbosity, -vv for debug")
                .action(ArgAction::Count),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let matches = new().try_get_matches_from(["pg_exporter"]);

        assert!(matches.is_ok());
    }

    #[test]
    fn test_new() {
        let command = new();

        assert_eq!(command.get_name(), "pg_exporter");
        assert_eq!(
            command.get_about().unwrap().to_string(),
            env!("CARGO_PKG_DESCRIPTION")
        );
        assert_eq!(
            command.get_version().unwrap().to_string(),
            env!("CARGO_PKG_VERSION")
        );
    }

    #[test]
    fn test_check_port_and_dsn() {
        let command = new();
        let matches = command.get_matches_from(vec![
            "pg_exporter",
            "--port",
            "8080",
            "--dsn",
            "postgres://user:password@localhost:5432/genesis",
        ]);

        assert_eq!(matches.get_one::<u16>("port").copied(), Some(8080));
        assert_eq!(
            matches.get_one::<String>("dsn").map(|s| s.to_string()),
            Some("postgres://user:password@localhost:5432/genesis".to_string())
        );
    }
}
