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
}
