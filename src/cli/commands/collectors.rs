use crate::collectors::{COLLECTOR_NAMES, Collector, all_factories};
use clap::{Arg, Command};

pub fn add_collectors_args(mut cmd: Command) -> Command {
    let factories = all_factories();

    for &name in COLLECTOR_NAMES {
        // Get the default enabled state from the collector
        let default_enabled = if let Some(factory) = factories.get(name) {
            let collector = factory();
            collector.enabled_by_default()
        } else {
            false // Fallback
        };

        // Create flag names
        let enable_flag: &'static str = Box::leak(format!("collector.{name}").into_boxed_str());
        let disable_flag: &'static str = Box::leak(format!("no-collector.{name}").into_boxed_str());

        // Create help text with default state indication only for enable flag
        let default_indicator = if default_enabled {
            " [default: enabled]"
        } else {
            " [default: disabled]"
        };
        let enable_help: &'static str =
            Box::leak(format!("Enable the {name} collector{default_indicator}").into_boxed_str());
        let disable_help: &'static str =
            Box::leak(format!("Disable the {name} collector").into_boxed_str());

        cmd = cmd
            .arg(
                Arg::new(enable_flag)
                    .long(enable_flag)
                    .help(enable_help)
                    .action(clap::ArgAction::SetTrue)
                    .default_value(if default_enabled { "true" } else { "false" }),
            )
            .arg(
                Arg::new(disable_flag)
                    .long(disable_flag)
                    .help(disable_help)
                    .action(clap::ArgAction::SetTrue)
                    .overrides_with(enable_flag),
            );
    }
    cmd
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::commands;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_all_collector_flags_are_added() {
        let cmd = commands::new();

        // Verify all collectors have both enable and disable flags
        for &name in COLLECTOR_NAMES {
            let enable_flag = format!("collector.{name}");
            let disable_flag = format!("no-collector.{name}");

            // Verify flags exist by trying to get them
            let matches = cmd
                .clone()
                .try_get_matches_from(vec!["pg_exporter"])
                .unwrap();

            // These should exist without error
            assert!(
                matches.contains_id(&enable_flag),
                "Missing enable flag for {name}"
            );
            assert!(
                matches.contains_id(&disable_flag),
                "Missing disable flag for {name}"
            );
        }
    }

    #[test]
    fn test_collector_default_values() {
        let cmd = commands::new();
        let matches = cmd.get_matches_from(vec!["pg_exporter"]);

        let factories = all_factories();

        for &name in COLLECTOR_NAMES {
            let enable_flag = format!("collector.{name}");

            if let Some(factory) = factories.get(name) {
                let collector = factory();
                let expected_default = collector.enabled_by_default();
                let actual_value = matches.get_flag(&enable_flag);

                assert_eq!(
                    actual_value, expected_default,
                    "Collector '{name}' default mismatch: expected {expected_default}, got {actual_value}"
                );
            }
        }
    }

    #[test]
    fn test_disable_flag_overrides_enable_flag() {
        let cmd = commands::new();

        // Test with a default-enabled collector (e.g., "default")
        let matches = cmd.get_matches_from(vec![
            "pg_exporter",
            "--collector.default",
            "--no-collector.default", // This comes last, so it wins
        ]);

        // The disable flag should override because it comes last
        assert!(
            matches.get_flag("no-collector.default"),
            "disable flag should be true"
        );
        // Note: The enable flag might still show as true due to default value,
        // but the disable flag takes precedence in the actual logic
    }

    #[test]
    fn test_enable_flag_after_disable_flag() {
        let cmd = commands::new();

        let matches = cmd.get_matches_from(vec![
            "pg_exporter",
            "--no-collector.default",
            "--collector.default", // This comes last, so it wins
        ]);

        // Enable flag comes last, so it should re-enable the collector
        assert!(
            matches.get_flag("collector.default"),
            "enable flag should be true"
        );
    }

    #[test]
    fn test_collector_toggle_behavior_in_dispatch() {
        use crate::cli::dispatch::get_enabled_collectors;

        let cmd = commands::new();

        // Test 1: Enable then disable (disable wins)
        let matches = cmd.clone().get_matches_from(vec![
            "pg_exporter",
            "--collector.default",
            "--no-collector.default",
        ]);
        let enabled = get_enabled_collectors(&matches);
        assert!(
            !enabled.contains(&"default".to_string()),
            "default should be disabled when disable flag comes last"
        );

        // Test 2: Disable then enable (enable wins)
        let matches = cmd.get_matches_from(vec![
            "pg_exporter",
            "--no-collector.default",
            "--collector.default",
        ]);
        let enabled = get_enabled_collectors(&matches);
        assert!(
            enabled.contains(&"default".to_string()),
            "default should be enabled when enable flag comes last"
        );
    }

    #[test]
    fn test_collector_flags_help_text() {
        let mut cmd = commands::new();
        let long_help = cmd.render_long_help().to_string();

        // Verify help text includes default indicators
        assert!(
            long_help.contains("[default: enabled]") || long_help.contains("[default: disabled]"),
            "Help text should indicate default states"
        );

        // Verify specific collectors are mentioned
        for &name in COLLECTOR_NAMES {
            assert!(
                long_help.contains(name),
                "Help text should mention collector '{name}'"
            );
        }
    }

    #[test]
    fn test_multiple_collectors_can_be_disabled() {
        use crate::cli::dispatch::get_enabled_collectors;

        let cmd = commands::new();
        let matches = cmd.get_matches_from(vec![
            "pg_exporter",
            "--no-collector.vacuum",
            "--no-collector.activity",
            "--no-collector.locks",
        ]);

        let enabled = get_enabled_collectors(&matches);

        // These collectors should NOT be in the enabled list
        assert!(!enabled.contains(&"vacuum".to_string()));
        assert!(!enabled.contains(&"activity".to_string()));
        assert!(!enabled.contains(&"locks".to_string()));

        // But default should still be there (unless also disabled)
        assert!(enabled.contains(&"default".to_string()));
    }

    #[test]
    fn test_enable_disabled_by_default_collector() {
        let cmd = commands::new();
        let factories = all_factories();

        // Find a collector that's disabled by default
        let disabled_collector = COLLECTOR_NAMES.iter().find(|&&name| {
            factories
                .get(name)
                .is_some_and(|f| !f().enabled_by_default())
        });

        if let Some(&name) = disabled_collector {
            let enable_flag = format!("--collector.{name}");
            let matches = cmd.get_matches_from(vec!["pg_exporter", &enable_flag]);

            assert!(
                matches.get_flag(&format!("collector.{name}")),
                "Should be able to enable disabled-by-default collector '{name}'"
            );
        }
    }
}
