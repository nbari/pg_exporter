use clap::{
    Arg, ArgAction, ColorChoice, Command,
    builder::styling::{AnsiColor, Effects, Styles},
};

mod collectors;

pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub fn new() -> Command {
    let styles = Styles::styled()
        .header(AnsiColor::Yellow.on_default() | Effects::BOLD)
        .usage(AnsiColor::Green.on_default() | Effects::BOLD)
        .literal(AnsiColor::Blue.on_default() | Effects::BOLD)
        .placeholder(AnsiColor::Green.on_default());

    let cmd = Command::new("pg_exporter")
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
                .default_value("postgresql://postgres@localhost:5432/postgres")
                .env("PG_EXPORTER_DSN")
                .value_name("DSN"),
        )
        .arg(
            Arg::new("exclude-databases")
                .long("exclude-databases")
                .help("Comma-separated list of databases to exclude (exact/case-sensitive)")
                .env("PG_EXPORTER_EXCLUDE_DATABASES")
                .value_name("template0,template1,...")
                .value_delimiter(',') // split CLI and env values by comma
                .action(ArgAction::Append), // allow repeated flags if desired
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Increase verbosity, -vv for debug")
                .action(ArgAction::Count),
        );

    collectors::add_collectors_args(cmd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        temp_env::with_var("PG_EXPORTER_DSN", None::<String>, || {
            let command = new();
            let matches = command.get_matches_from(vec!["pg_exporter"]);

            assert_eq!(matches.get_one::<u16>("port").copied(), Some(9432));
            assert_eq!(
                matches.get_one::<String>("dsn").map(|s| s.to_string()),
                Some("postgresql://postgres@localhost:5432/postgres".to_string())
            );
        });
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
            "--exclude-databases",
            "template0,template1",
            "--exclude-databases",
            "postgres",
        ]);

        assert_eq!(matches.get_one::<u16>("port").copied(), Some(8080));
        assert_eq!(
            matches.get_one::<String>("dsn").map(|s| s.to_string()),
            Some("postgres://user:password@localhost:5432/genesis".to_string())
        );

        let excludes: Vec<String> = matches
            .get_many::<String>("exclude-databases")
            .unwrap()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(excludes, vec!["template0", "template1", "postgres"]);
    }

    #[test]
    fn test_check_exclude_databases_env() {
        temp_env::with_var("PG_EXPORTER_EXCLUDE_DATABASES", Some("db1,db2,db3"), || {
            let command = new();
            let matches = command.get_matches_from(vec!["pg_exporter"]);

            let excludes: Vec<String> = matches
                .get_many::<String>("exclude-databases")
                .unwrap()
                .map(|s| s.to_string())
                .collect();
            assert_eq!(excludes, vec!["db1", "db2", "db3"]);
        });
    }
}
