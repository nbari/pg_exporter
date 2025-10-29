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

    let git_hash = built_info::GIT_COMMIT_HASH.unwrap_or("unknown");
    let long_version: &'static str =
        Box::leak(format!("{} - {}", env!("CARGO_PKG_VERSION"), git_hash).into_boxed_str());

    let cmd = Command::new("pg_exporter")
        .about("PostgreSQL metric exporter for Prometheus")
        .version(env!("CARGO_PKG_VERSION"))
        .long_version(long_version)
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
            Arg::new("listen")
                .short('l')
                .long("listen")
                .help("IP address to bind to (default: [::]:port, accepts both IPv6 and IPv4)")
                .long_help(
                    "IP address to bind to:\n\
                     - Not specified (default): Binds to [::]:port which accepts both IPv6 and IPv4 connections.\n\
                       Falls back to 0.0.0.0:port if IPv6 is not available on the system.\n\
                     - Specific IPv4: e.g., '0.0.0.0', '127.0.0.1', '192.168.1.100'\n\
                     - Specific IPv6: e.g., '::', '::1', 'fe80::1'\n\n\
                     Examples:\n\
                       --listen 0.0.0.0       Bind to all IPv4 interfaces only\n\
                       --listen 127.0.0.1     Bind to localhost IPv4 only\n\
                       --listen ::            Bind to all IPv6 interfaces (typically accepts IPv4 too)\n\
                       --listen ::1           Bind to localhost IPv6 only\n\n\
                     Note: Binding to [::] (IPv6 all interfaces) usually accepts both IPv6 and\n\
                     IPv4 connections through IPv4-mapped IPv6 addresses on dual-stack systems.",
                )
                .env("PG_EXPORTER_LISTEN")
                .value_name("IP"),
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

    #[test]
    fn test_verbose_flag_single() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "-v"]);
        assert_eq!(matches.get_count("verbose"), 1);
    }

    #[test]
    fn test_verbose_flag_double() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "-vv"]);
        assert_eq!(matches.get_count("verbose"), 2);
    }

    #[test]
    fn test_verbose_flag_triple() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "-vvv"]);
        assert_eq!(matches.get_count("verbose"), 3);
    }

    #[test]
    fn test_verbose_flag_long_form() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "--verbose", "--verbose"]);
        assert_eq!(matches.get_count("verbose"), 2);
    }

    #[test]
    fn test_port_short_flag() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "-p", "8080"]);
        assert_eq!(matches.get_one::<u16>("port").copied(), Some(8080));
    }

    #[test]
    fn test_port_validation_min() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "--port", "1"]);
        assert_eq!(matches.get_one::<u16>("port").copied(), Some(1));
    }

    #[test]
    fn test_port_validation_max() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "--port", "65535"]);
        assert_eq!(matches.get_one::<u16>("port").copied(), Some(65535));
    }

    #[test]
    fn test_port_validation_invalid() {
        let command = new();
        let result = command.try_get_matches_from(vec!["pg_exporter", "--port", "99999"]);
        assert!(result.is_err(), "Should reject port > 65535");
    }

    #[test]
    fn test_port_validation_non_numeric() {
        let command = new();
        let result = command.try_get_matches_from(vec!["pg_exporter", "--port", "abc"]);
        assert!(result.is_err(), "Should reject non-numeric port");
    }

    #[test]
    fn test_port_from_env() {
        temp_env::with_var("PG_EXPORTER_PORT", Some("7777"), || {
            let command = new();
            let matches = command.get_matches_from(vec!["pg_exporter"]);
            assert_eq!(matches.get_one::<u16>("port").copied(), Some(7777));
        });
    }

    #[test]
    fn test_port_cli_overrides_env() {
        temp_env::with_var("PG_EXPORTER_PORT", Some("7777"), || {
            let command = new();
            let matches = command.get_matches_from(vec!["pg_exporter", "--port", "8888"]);
            assert_eq!(matches.get_one::<u16>("port").copied(), Some(8888));
        });
    }

    #[test]
    fn test_dsn_with_special_characters() {
        let command = new();
        let matches = command.get_matches_from(vec![
            "pg_exporter",
            "--dsn",
            "postgres://user:p@ss%20word@host:5432/db?sslmode=require",
        ]);

        assert_eq!(
            matches.get_one::<String>("dsn").map(|s| s.to_string()),
            Some("postgres://user:p@ss%20word@host:5432/db?sslmode=require".to_string())
        );
    }

    #[test]
    fn test_dsn_from_env() {
        temp_env::with_var(
            "PG_EXPORTER_DSN",
            Some("postgres://custom:5432/mydb"),
            || {
                let command = new();
                let matches = command.get_matches_from(vec!["pg_exporter"]);

                assert_eq!(
                    matches.get_one::<String>("dsn").map(|s| s.to_string()),
                    Some("postgres://custom:5432/mydb".to_string())
                );
            },
        );
    }

    #[test]
    fn test_dsn_cli_overrides_env() {
        temp_env::with_var("PG_EXPORTER_DSN", Some("postgres://env:5432/db"), || {
            let command = new();
            let matches =
                command.get_matches_from(vec!["pg_exporter", "--dsn", "postgres://cli:5432/db"]);

            assert_eq!(
                matches.get_one::<String>("dsn").map(|s| s.to_string()),
                Some("postgres://cli:5432/db".to_string())
            );
        });
    }

    #[test]
    fn test_exclude_databases_multiple_flags() {
        let command = new();
        let matches = command.get_matches_from(vec![
            "pg_exporter",
            "--exclude-databases",
            "db1",
            "--exclude-databases",
            "db2",
            "--exclude-databases",
            "db3",
        ]);

        let excludes: Vec<String> = matches
            .get_many::<String>("exclude-databases")
            .unwrap()
            .map(|s| s.to_string())
            .collect();

        assert_eq!(excludes, vec!["db1", "db2", "db3"]);
    }

    #[test]
    fn test_exclude_databases_comma_separated_single_flag() {
        let command = new();
        let matches =
            command.get_matches_from(vec!["pg_exporter", "--exclude-databases", "db1,db2,db3"]);

        let excludes: Vec<String> = matches
            .get_many::<String>("exclude-databases")
            .unwrap()
            .map(|s| s.to_string())
            .collect();

        assert_eq!(excludes, vec!["db1", "db2", "db3"]);
    }

    #[test]
    fn test_exclude_databases_with_spaces() {
        let command = new();
        let matches = command.get_matches_from(vec![
            "pg_exporter",
            "--exclude-databases",
            " db1 , db2 , db3 ",
        ]);

        let excludes: Vec<String> = matches
            .get_many::<String>("exclude-databases")
            .unwrap()
            .map(|s| s.trim().to_string())
            .collect();

        assert_eq!(excludes, vec!["db1", "db2", "db3"]);
    }

    #[test]
    fn test_exclude_databases_mixed_flags_and_commas() {
        let command = new();
        let matches = command.get_matches_from(vec![
            "pg_exporter",
            "--exclude-databases",
            "db1,db2",
            "--exclude-databases",
            "db3",
        ]);

        let excludes: Vec<String> = matches
            .get_many::<String>("exclude-databases")
            .unwrap()
            .map(|s| s.to_string())
            .collect();

        assert_eq!(excludes, vec!["db1", "db2", "db3"]);
    }

    #[test]
    fn test_long_version_includes_git_hash() {
        let command = new();
        let long_version = command.get_long_version().unwrap().to_string();

        // Should include version and git hash separated by " - "
        assert!(long_version.contains(env!("CARGO_PKG_VERSION")));
        assert!(long_version.contains(" - "));
    }

    #[test]
    fn test_command_name() {
        let command = new();
        assert_eq!(command.get_name(), "pg_exporter");
    }

    #[test]
    fn test_command_has_port_argument() {
        let command = new();
        let port_arg = command.get_arguments().find(|arg| arg.get_id() == "port");
        assert!(port_arg.is_some(), "Command should have 'port' argument");
    }

    #[test]
    fn test_command_has_dsn_argument() {
        let command = new();
        let dsn_arg = command.get_arguments().find(|arg| arg.get_id() == "dsn");
        assert!(dsn_arg.is_some(), "Command should have 'dsn' argument");
    }

    #[test]
    fn test_command_has_verbose_argument() {
        let command = new();
        let verbose_arg = command
            .get_arguments()
            .find(|arg| arg.get_id() == "verbose");
        assert!(
            verbose_arg.is_some(),
            "Command should have 'verbose' argument"
        );
    }

    #[test]
    fn test_command_has_exclude_databases_argument() {
        let command = new();
        let exclude_arg = command
            .get_arguments()
            .find(|arg| arg.get_id() == "exclude-databases");
        assert!(
            exclude_arg.is_some(),
            "Command should have 'exclude-databases' argument"
        );
    }

    #[test]
    fn test_listen_default() {
        temp_env::with_var("PG_EXPORTER_LISTEN", None::<String>, || {
            let command = new();
            let matches = command.get_matches_from(vec!["pg_exporter"]);
            assert_eq!(matches.get_one::<String>("listen"), None);
        });
    }

    #[test]
    fn test_listen_ipv4_all() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "--listen", "0.0.0.0"]);
        assert_eq!(
            matches.get_one::<String>("listen").map(|s| s.as_str()),
            Some("0.0.0.0")
        );
    }

    #[test]
    fn test_listen_ipv4_localhost() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "--listen", "127.0.0.1"]);
        assert_eq!(
            matches.get_one::<String>("listen").map(|s| s.as_str()),
            Some("127.0.0.1")
        );
    }

    #[test]
    fn test_listen_ipv4_specific() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "--listen", "192.168.1.100"]);
        assert_eq!(
            matches.get_one::<String>("listen").map(|s| s.as_str()),
            Some("192.168.1.100")
        );
    }

    #[test]
    fn test_listen_ipv6_all() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "--listen", "::"]);
        assert_eq!(
            matches.get_one::<String>("listen").map(|s| s.as_str()),
            Some("::")
        );
    }

    #[test]
    fn test_listen_ipv6_localhost() {
        let command = new();
        let matches = command.get_matches_from(vec!["pg_exporter", "--listen", "::1"]);
        assert_eq!(
            matches.get_one::<String>("listen").map(|s| s.as_str()),
            Some("::1")
        );
    }

    #[test]
    fn test_listen_from_env() {
        temp_env::with_var("PG_EXPORTER_LISTEN", Some("192.168.1.1"), || {
            let command = new();
            let matches = command.get_matches_from(vec!["pg_exporter"]);
            assert_eq!(
                matches.get_one::<String>("listen").map(|s| s.as_str()),
                Some("192.168.1.1")
            );
        });
    }

    #[test]
    fn test_listen_cli_overrides_env() {
        temp_env::with_var("PG_EXPORTER_LISTEN", Some("::1"), || {
            let command = new();
            let matches = command.get_matches_from(vec!["pg_exporter", "--listen", "127.0.0.1"]);
            assert_eq!(
                matches.get_one::<String>("listen").map(|s| s.as_str()),
                Some("127.0.0.1")
            );
        });
    }
}
