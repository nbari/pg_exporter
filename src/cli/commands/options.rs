use clap::{Arg, Command, value_parser};
use std::num::NonZeroUsize;

pub fn add_collector_option_args(cmd: Command) -> Command {
    cmd.arg(
        Arg::new("statements.top-n")
            .long("statements.top-n")
            .help("Number of pg_stat_statements rows to expose")
            .long_help(
                "Number of pg_stat_statements rows to expose.\n\n\
                 This limits the exporter-side top-N query set ordered by total execution time.\n\
                 Lower values reduce cardinality and scrape cost; higher values provide more query coverage.\n\n\
                 Examples:\n\
                   --statements.top-n 10\n\
                   --statements.top-n 25\n\
                   PG_EXPORTER_STATEMENTS_TOP_N=50",
            )
            .env("PG_EXPORTER_STATEMENTS_TOP_N")
            .default_value("25")
            .value_name("N")
            .value_parser(value_parser!(NonZeroUsize)),
    )
    .arg(
        Arg::new("collectors.max-db-concurrency")
            .long("collectors.max-db-concurrency")
            .help("Max databases queried concurrently per multi-database collector")
            .long_help(
                "Maximum number of databases queried concurrently within a single scrape of a \
                 multi-database collector (stat, index).\n\n\
                 A PostgreSQL connection is bound to one database, so these collectors open one \
                 connection per database. This caps how many run at once, keeping peak connections \
                 independent of the number of databases in the cluster (important on instances with a \
                 low, shared max_connections such as AWS RDS).\n\n\
                 Lower values are gentler on connection limits; higher values make scrapes faster on \
                 clusters with many databases (at the cost of more concurrent connections).\n\n\
                 Examples:\n\
                   --collectors.max-db-concurrency 5\n\
                   --collectors.max-db-concurrency 20\n\
                   PG_EXPORTER_MAX_DB_CONCURRENCY=3",
            )
            .env("PG_EXPORTER_MAX_DB_CONCURRENCY")
            .default_value(MAX_DB_CONCURRENCY_DEFAULT)
            .value_name("N")
            .value_parser(value_parser!(NonZeroUsize)),
    )
}

/// String form of the default max per-database concurrency, kept in sync with
/// [`crate::collectors::MAX_DB_QUERY_CONCURRENCY`] by `max_db_concurrency_default_matches_const`.
const MAX_DB_CONCURRENCY_DEFAULT: &str = "5";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::commands;

    #[test]
    fn test_statements_top_n_default() {
        temp_env::with_var("PG_EXPORTER_STATEMENTS_TOP_N", None::<String>, || {
            let matches = commands::new().get_matches_from(vec!["pg_exporter"]);
            assert_eq!(
                matches
                    .get_one::<NonZeroUsize>("statements.top-n")
                    .map(|value| value.get()),
                Some(25)
            );
        });
    }

    #[test]
    fn test_statements_top_n_from_env() {
        temp_env::with_var("PG_EXPORTER_STATEMENTS_TOP_N", Some("40"), || {
            let matches = commands::new().get_matches_from(vec!["pg_exporter"]);
            assert_eq!(
                matches
                    .get_one::<NonZeroUsize>("statements.top-n")
                    .map(|value| value.get()),
                Some(40)
            );
        });
    }

    #[test]
    fn test_statements_top_n_cli_overrides_env() {
        temp_env::with_var("PG_EXPORTER_STATEMENTS_TOP_N", Some("40"), || {
            let matches =
                commands::new().get_matches_from(vec!["pg_exporter", "--statements.top-n", "15"]);
            assert_eq!(
                matches
                    .get_one::<NonZeroUsize>("statements.top-n")
                    .map(|value| value.get()),
                Some(15)
            );
        });
    }

    #[test]
    fn test_statements_top_n_rejects_zero() {
        let result =
            commands::new().try_get_matches_from(vec!["pg_exporter", "--statements.top-n", "0"]);
        assert!(result.is_err(), "Should reject non-positive top-n values");
    }

    #[test]
    fn test_statements_top_n_rejects_non_numeric_input() {
        let result =
            commands::new().try_get_matches_from(vec!["pg_exporter", "--statements.top-n", "AAA"]);
        assert!(result.is_err(), "Should reject non-numeric top-n values");
    }

    #[test]
    fn test_add_collector_option_args_registers_flag() {
        let mut cmd = add_collector_option_args(Command::new("pg_exporter"));
        let help = cmd.render_long_help().to_string();
        assert!(help.contains("--statements.top-n"));
    }

    #[test]
    fn test_max_db_concurrency_default() {
        temp_env::with_var("PG_EXPORTER_MAX_DB_CONCURRENCY", None::<String>, || {
            let matches = commands::new().get_matches_from(vec!["pg_exporter"]);
            assert_eq!(
                matches
                    .get_one::<NonZeroUsize>("collectors.max-db-concurrency")
                    .map(|value| value.get()),
                Some(5)
            );
        });
    }

    #[test]
    fn test_max_db_concurrency_from_env() {
        temp_env::with_var("PG_EXPORTER_MAX_DB_CONCURRENCY", Some("12"), || {
            let matches = commands::new().get_matches_from(vec!["pg_exporter"]);
            assert_eq!(
                matches
                    .get_one::<NonZeroUsize>("collectors.max-db-concurrency")
                    .map(|value| value.get()),
                Some(12)
            );
        });
    }

    #[test]
    fn test_max_db_concurrency_cli_overrides_env() {
        temp_env::with_var("PG_EXPORTER_MAX_DB_CONCURRENCY", Some("12"), || {
            let matches = commands::new().get_matches_from(vec![
                "pg_exporter",
                "--collectors.max-db-concurrency",
                "3",
            ]);
            assert_eq!(
                matches
                    .get_one::<NonZeroUsize>("collectors.max-db-concurrency")
                    .map(|value| value.get()),
                Some(3)
            );
        });
    }

    #[test]
    fn test_max_db_concurrency_rejects_zero() {
        let result = commands::new().try_get_matches_from(vec![
            "pg_exporter",
            "--collectors.max-db-concurrency",
            "0",
        ]);
        assert!(result.is_err(), "Should reject a zero concurrency limit");
    }

    #[test]
    fn max_db_concurrency_default_matches_const() {
        // The CLI default string must stay in sync with the compile-time fallback constant.
        assert_eq!(
            MAX_DB_CONCURRENCY_DEFAULT.parse::<usize>().ok(),
            Some(crate::collectors::MAX_DB_QUERY_CONCURRENCY)
        );
    }

    /// Parses `--collectors.max-db-concurrency=<value>` and returns whether clap rejected it.
    /// The `=` form is used so values beginning with `-` are treated as the flag's value
    /// rather than a separate option.
    fn max_db_concurrency_cli_is_rejected(value: &str) -> bool {
        commands::new()
            .try_get_matches_from(vec![
                "pg_exporter".to_string(),
                format!("--collectors.max-db-concurrency={value}"),
            ])
            .is_err()
    }

    #[test]
    fn max_db_concurrency_rejects_invalid_cli_values() {
        // Zero, negatives, floats, non-numeric, empty, whitespace-padded, hex, and overflow
        // must all be rejected at parse time so an invalid limit can never reach a collector.
        //
        // This also covers the env path: the CLI flag and `PG_EXPORTER_MAX_DB_CONCURRENCY`
        // share the exact same `value_parser!(NonZeroUsize)`, so a value rejected here is
        // rejected identically when supplied via the environment. (We validate invalid values
        // through the CLI rather than the environment because this is a shared-process test
        // binary: many other tests call clap's `get_matches_from`, which exits the process on a
        // parse error, so injecting a bad value into the real environment would abort them.)
        for value in [
            "0",
            "-1",
            "-5",
            "2.5",
            "abc",
            "",
            " 5",
            "5 ",
            "5x",
            "0x10",
            "1e3",
            "+",
            "  ",
            "99999999999999999999999999999999999999",
        ] {
            assert!(
                max_db_concurrency_cli_is_rejected(value),
                "CLI value {value:?} should be rejected"
            );
        }
    }

    #[test]
    fn max_db_concurrency_accepts_valid_boundaries() {
        for (value, expected) in [("1", 1usize), ("5", 5), ("50", 50), ("4096", 4096)] {
            let matches = commands::new().get_matches_from(vec![
                "pg_exporter".to_string(),
                format!("--collectors.max-db-concurrency={value}"),
            ]);
            assert_eq!(
                matches
                    .get_one::<NonZeroUsize>("collectors.max-db-concurrency")
                    .map(|parsed| parsed.get()),
                Some(expected),
                "CLI value {value} should parse to {expected}"
            );
        }
    }

    #[test]
    fn max_db_concurrency_accepts_valid_env_values() {
        for (value, expected) in [("1", 1usize), ("7", 7), ("64", 64)] {
            temp_env::with_var("PG_EXPORTER_MAX_DB_CONCURRENCY", Some(value), || {
                let matches = commands::new().get_matches_from(vec!["pg_exporter"]);
                assert_eq!(
                    matches
                        .get_one::<NonZeroUsize>("collectors.max-db-concurrency")
                        .map(|parsed| parsed.get()),
                    Some(expected),
                    "env value {value} should parse to {expected}"
                );
            });
        }
    }
}
