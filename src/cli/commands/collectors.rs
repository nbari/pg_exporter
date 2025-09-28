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
        let enable_flag: &'static str = Box::leak(format!("collector.{}", name).into_boxed_str());
        let disable_flag: &'static str =
            Box::leak(format!("no-collector.{}", name).into_boxed_str());

        // Create help text with default state indication only for enable flag
        let default_indicator = if default_enabled {
            " [default: enabled]"
        } else {
            " [default: disabled]"
        };
        let enable_help: &'static str = Box::leak(
            format!("Enable the {} collector{}", name, default_indicator).into_boxed_str(),
        );
        let disable_help: &'static str =
            Box::leak(format!("Disable the {} collector", name).into_boxed_str());

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
