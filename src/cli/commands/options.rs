use clap::{Arg, Command, value_parser};
use std::num::{NonZeroU64, NonZeroUsize};

use crate::collectors::MAX_DB_QUERY_CONCURRENCY_LIMIT;

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
    .arg(max_db_concurrency_arg())
    .arg(connect_timeout_arg())
    .arg(
        Arg::new("scrape.lock-timeout-ms")
            .long("scrape.lock-timeout-ms")
            .help("Default PostgreSQL lock_timeout for scrape connections, in milliseconds")
            .long_help(
                "Default PostgreSQL lock_timeout injected into scrape connections, in milliseconds.\n\n\
                 This aborts lock-blocked scrape queries server-side so blocked backends release \
                 their connection slots instead of accumulating across scrapes. The DSN/PGOPTIONS \
                 value wins when it explicitly sets lock_timeout, including lock_timeout=0.\n\n\
                 Examples:\n\
                   --scrape.lock-timeout-ms 2000\n\
                   --scrape.lock-timeout-ms 5000\n\
                   PG_EXPORTER_LOCK_TIMEOUT_MS=1000",
            )
            .env("PG_EXPORTER_LOCK_TIMEOUT_MS")
            .default_value(LOCK_TIMEOUT_MS_DEFAULT)
            .value_name("MS")
            .value_parser(value_parser!(NonZeroU64)),
    )
    .arg(
        Arg::new("scrape.statement-timeout-ms")
            .long("scrape.statement-timeout-ms")
            .help("Default PostgreSQL statement_timeout for scrape connections, in milliseconds")
            .long_help(
                "Default PostgreSQL statement_timeout injected into scrape connections, in milliseconds.\n\n\
                 This is the server-side backstop for slow or stuck scrape queries after they start \
                 running. It must be positive and lower than --scrape.timeout-ms; statement_timeout=0 \
                 is rejected because it disables the timeout.\n\n\
                 Examples:\n\
                   --scrape.statement-timeout-ms 10000\n\
                   --scrape.statement-timeout-ms 30000\n\
                   PG_EXPORTER_STATEMENT_TIMEOUT_MS=15000",
            )
            .env("PG_EXPORTER_STATEMENT_TIMEOUT_MS")
            .default_value(STATEMENT_TIMEOUT_MS_DEFAULT)
            .value_name("MS")
            .value_parser(value_parser!(NonZeroU64)),
    )
    .arg(
        Arg::new("scrape.timeout-ms")
            .long("scrape.timeout-ms")
            .help("Whole /metrics scrape timeout, in milliseconds")
            .long_help(
                "Whole /metrics scrape timeout, in milliseconds.\n\n\
                 This bounds the HTTP scrape wall-clock duration. It must be longer than \
                 statement_timeout so PostgreSQL aborts individual queries before the exporter aborts \
                 the scrape. It must be positive.\n\n\
                 Examples:\n\
                   --scrape.timeout-ms 15000\n\
                   --scrape.timeout-ms 60000\n\
                   PG_EXPORTER_SCRAPE_TIMEOUT_MS=20000",
            )
            .env("PG_EXPORTER_SCRAPE_TIMEOUT_MS")
            .default_value(SCRAPE_TIMEOUT_MS_DEFAULT)
            .value_name("MS")
            .value_parser(value_parser!(NonZeroU64)),
    )
}

fn max_db_concurrency_arg() -> Arg {
    Arg::new("collectors.max-db-concurrency")
        .long("collectors.max-db-concurrency")
        .help("Max non-default databases queried concurrently across all collectors")
        .long_help(
            "Maximum number of non-default databases queried concurrently across all \
             multi-database collectors (stat, index, vacuum name resolution).\n\n\
             A PostgreSQL connection is bound to one database, so these collectors open one \
             ephemeral connection per non-default database query. This caps how many run at \
             once globally, keeping peak exporter connections bounded to the shared pool (3) \
             plus this value, independent of the number of databases in the cluster (important \
             on instances with a low, shared max_connections such as AWS RDS).\n\n\
             Valid values are 1 through 16. Lower values are gentler on connection limits; \
             higher values make scrapes faster on clusters with many databases at the cost of \
             more concurrent connections.\n\n\
             Examples:\n\
               --collectors.max-db-concurrency 2\n\
               --collectors.max-db-concurrency 8\n\
               PG_EXPORTER_MAX_DB_CONCURRENCY=1",
        )
        .env("PG_EXPORTER_MAX_DB_CONCURRENCY")
        .default_value(MAX_DB_CONCURRENCY_DEFAULT)
        .value_name("N")
        .value_parser(parse_max_db_concurrency)
}

fn connect_timeout_arg() -> Arg {
    Arg::new("scrape.connect-timeout-ms")
        .long("scrape.connect-timeout-ms")
        .help("PostgreSQL connection-establishment timeout, in milliseconds")
        .long_help(
            "PostgreSQL connection-establishment timeout, in milliseconds.\n\n\
             This bounds DNS, TCP, TLS, authentication, and shared-pool connection acquisition \
             before any server-side PostgreSQL timeout can apply. It must be positive and lower \
             than --scrape.timeout-ms.\n\n\
             Examples:\n\
               --scrape.connect-timeout-ms 5000\n\
               --scrape.connect-timeout-ms 10000\n\
               PG_EXPORTER_CONNECT_TIMEOUT_MS=3000",
        )
        .env("PG_EXPORTER_CONNECT_TIMEOUT_MS")
        .default_value(CONNECT_TIMEOUT_MS_DEFAULT)
        .value_name("MS")
        .value_parser(value_parser!(NonZeroU64))
}

/// String form of the default max per-database concurrency, kept in sync with
/// [`crate::collectors::MAX_DB_QUERY_CONCURRENCY`] by `max_db_concurrency_default_matches_const`.
const MAX_DB_CONCURRENCY_DEFAULT: &str = "2";
const CONNECT_TIMEOUT_MS_DEFAULT: &str = "5000";
const LOCK_TIMEOUT_MS_DEFAULT: &str = "2000";
const STATEMENT_TIMEOUT_MS_DEFAULT: &str = "10000";
const SCRAPE_TIMEOUT_MS_DEFAULT: &str = "15000";

fn parse_max_db_concurrency(value: &str) -> Result<NonZeroUsize, String> {
    let parsed = value.parse::<NonZeroUsize>().map_err(|_| {
        format!(
            "database concurrency must be an integer between 1 and {MAX_DB_QUERY_CONCURRENCY_LIMIT}"
        )
    })?;

    if parsed.get() > MAX_DB_QUERY_CONCURRENCY_LIMIT {
        return Err(format!(
            "database concurrency must be between 1 and {MAX_DB_QUERY_CONCURRENCY_LIMIT}"
        ));
    }

    Ok(parsed)
}

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
                Some(2)
            );
        });
    }

    #[test]
    fn test_scrape_timeout_defaults() {
        temp_env::with_var("PG_EXPORTER_CONNECT_TIMEOUT_MS", None::<String>, || {
            temp_env::with_var("PG_EXPORTER_LOCK_TIMEOUT_MS", None::<String>, || {
                temp_env::with_var("PG_EXPORTER_STATEMENT_TIMEOUT_MS", None::<String>, || {
                    temp_env::with_var("PG_EXPORTER_SCRAPE_TIMEOUT_MS", None::<String>, || {
                        let matches = commands::new().get_matches_from(vec!["pg_exporter"]);

                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.connect-timeout-ms")
                                .map(|value| value.get()),
                            Some(5_000)
                        );
                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.lock-timeout-ms")
                                .map(|value| value.get()),
                            Some(2_000)
                        );
                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.statement-timeout-ms")
                                .map(|value| value.get()),
                            Some(10_000)
                        );
                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.timeout-ms")
                                .map(|value| value.get()),
                            Some(15_000)
                        );
                    });
                });
            });
        });
    }

    #[test]
    fn test_scrape_timeouts_from_env() {
        temp_env::with_var("PG_EXPORTER_CONNECT_TIMEOUT_MS", Some("7000"), || {
            temp_env::with_var("PG_EXPORTER_LOCK_TIMEOUT_MS", Some("3000"), || {
                temp_env::with_var("PG_EXPORTER_STATEMENT_TIMEOUT_MS", Some("20000"), || {
                    temp_env::with_var("PG_EXPORTER_SCRAPE_TIMEOUT_MS", Some("25000"), || {
                        let matches = commands::new().get_matches_from(vec!["pg_exporter"]);

                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.connect-timeout-ms")
                                .map(|value| value.get()),
                            Some(7_000)
                        );
                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.lock-timeout-ms")
                                .map(|value| value.get()),
                            Some(3_000)
                        );
                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.statement-timeout-ms")
                                .map(|value| value.get()),
                            Some(20_000)
                        );
                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.timeout-ms")
                                .map(|value| value.get()),
                            Some(25_000)
                        );
                    });
                });
            });
        });
    }

    #[test]
    fn test_scrape_timeout_cli_overrides_env() {
        temp_env::with_var("PG_EXPORTER_CONNECT_TIMEOUT_MS", Some("7000"), || {
            temp_env::with_var("PG_EXPORTER_LOCK_TIMEOUT_MS", Some("3000"), || {
                temp_env::with_var("PG_EXPORTER_STATEMENT_TIMEOUT_MS", Some("20000"), || {
                    temp_env::with_var("PG_EXPORTER_SCRAPE_TIMEOUT_MS", Some("25000"), || {
                        let matches = commands::new().get_matches_from(vec![
                            "pg_exporter",
                            "--scrape.connect-timeout-ms",
                            "8000",
                            "--scrape.lock-timeout-ms",
                            "4000",
                            "--scrape.statement-timeout-ms",
                            "30000",
                            "--scrape.timeout-ms",
                            "35000",
                        ]);

                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.connect-timeout-ms")
                                .map(|value| value.get()),
                            Some(8_000)
                        );
                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.lock-timeout-ms")
                                .map(|value| value.get()),
                            Some(4_000)
                        );
                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.statement-timeout-ms")
                                .map(|value| value.get()),
                            Some(30_000)
                        );
                        assert_eq!(
                            matches
                                .get_one::<NonZeroU64>("scrape.timeout-ms")
                                .map(|value| value.get()),
                            Some(35_000)
                        );
                    });
                });
            });
        });
    }

    #[test]
    fn test_scrape_timeouts_reject_zero() {
        for flag in [
            "--scrape.connect-timeout-ms",
            "--scrape.lock-timeout-ms",
            "--scrape.statement-timeout-ms",
            "--scrape.timeout-ms",
        ] {
            let result = commands::new().try_get_matches_from(vec!["pg_exporter", flag, "0"]);
            assert!(result.is_err(), "Should reject zero for {flag}");
        }
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

    #[test]
    fn scrape_timeout_defaults_match_consts() {
        assert_eq!(
            CONNECT_TIMEOUT_MS_DEFAULT.parse::<u64>().ok(),
            Some(crate::collectors::DEFAULT_CONNECT_TIMEOUT_MS)
        );
        assert_eq!(
            LOCK_TIMEOUT_MS_DEFAULT.parse::<u64>().ok(),
            Some(crate::collectors::DEFAULT_LOCK_TIMEOUT_MS)
        );
        assert_eq!(
            STATEMENT_TIMEOUT_MS_DEFAULT.parse::<u64>().ok(),
            Some(crate::collectors::DEFAULT_STATEMENT_TIMEOUT_MS)
        );
        assert_eq!(
            SCRAPE_TIMEOUT_MS_DEFAULT.parse::<u64>().ok(),
            Some(crate::collectors::DEFAULT_SCRAPE_TIMEOUT_MS)
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
        // share the exact same parser, so a value rejected here is
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
            "17",
            "50",
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
        for (value, expected) in [("1", 1usize), ("2", 2), ("8", 8), ("16", 16)] {
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
        for (value, expected) in [("1", 1usize), ("2", 2), ("7", 7), ("16", 16)] {
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
