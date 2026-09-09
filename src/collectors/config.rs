use crate::collectors::{COLLECTOR_NAMES, system::ProcessMemorySource};
use std::{collections::HashSet, time::Duration};

/// Default minimum delay between two `pg_stat_statements` query-text lookups.
pub const DEFAULT_STATEMENTS_QUERY_TEXT_REFRESH: Duration = Duration::from_mins(15);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatementsConfig {
    pub top_n: usize,
    /// Minimum delay between two query-text lookups, or `None` when the exporter must
    /// never read `pg_stat_statements` query texts.
    pub query_text_refresh: Option<Duration>,
}

/// Default `pg_sequences` used-ratio required for a sequence to be exported.
pub const DEFAULT_SEQUENCES_MIN_RATIO: f64 = 0.5;

#[derive(Clone, Debug, PartialEq)]
pub struct SequencesConfig {
    /// Only sequences whose `last_value / max_value` ratio is at least this value are
    /// exported, keeping cardinality bounded so a healthy database exports nothing.
    pub min_ratio: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SystemConfig {
    /// Where `--collector.system` reads process-group memory from. Defaults to private
    /// RSS (`resident − shared` from `statm`); PSS costs
    /// `O(processes × resident pages)`. See issues #35 and #36.
    pub process_memory: ProcessMemorySource,
}

#[derive(Clone, Debug)]
pub struct CollectorConfig {
    pub enabled_collectors: HashSet<String>,
    pub statements: StatementsConfig,
    pub sequences: SequencesConfig,
    pub system: SystemConfig,
}

impl CollectorConfig {
    /// Create an empty config with an explicit statements top-N value.
    #[must_use]
    pub fn new(statements_top_n: usize) -> Self {
        Self {
            enabled_collectors: HashSet::new(),
            statements: StatementsConfig {
                top_n: statements_top_n,
                query_text_refresh: Some(DEFAULT_STATEMENTS_QUERY_TEXT_REFRESH),
            },
            sequences: SequencesConfig {
                min_ratio: DEFAULT_SEQUENCES_MIN_RATIO,
            },
            system: SystemConfig {
                process_memory: ProcessMemorySource::default(),
            },
        }
    }

    /// Set where `--collector.system` reads process-group memory from.
    #[must_use]
    pub const fn with_system_process_memory(mut self, source: ProcessMemorySource) -> Self {
        self.system.process_memory = source;
        self
    }

    /// Set the minimum delay between two `pg_stat_statements` query-text lookups.
    ///
    /// `None` disables text lookups entirely.
    #[must_use]
    pub const fn with_statements_query_text_refresh(mut self, refresh: Option<Duration>) -> Self {
        self.statements.query_text_refresh = refresh;
        self
    }

    /// Set the minimum `pg_sequences` used-ratio for the sequences collector.
    #[must_use]
    pub fn with_sequences_min_ratio(mut self, min_ratio: f64) -> Self {
        self.sequences.min_ratio = min_ratio;
        self
    }

    /// Enable collectors by name
    #[must_use]
    pub fn with_enabled(mut self, collectors: &[String]) -> Self {
        self.enabled_collectors = collectors.iter().cloned().collect();
        self
    }

    /// Check if a collector is enabled
    #[must_use]
    pub fn is_enabled(&self, name: &str) -> bool {
        self.enabled_collectors.contains(name)
    }

    /// Return enabled collector names in the registry/CLI display order.
    #[must_use]
    pub fn enabled_collectors_in_order(&self) -> Vec<String> {
        COLLECTOR_NAMES
            .iter()
            .filter(|name| self.enabled_collectors.contains(**name))
            .map(|name| (*name).to_string())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_sets_statements_top_n() {
        let config = CollectorConfig::new(25);
        assert_eq!(config.statements.top_n, 25);
    }

    #[test]
    fn test_new_sets_default_query_text_refresh() {
        let config = CollectorConfig::new(25);
        assert_eq!(
            config.statements.query_text_refresh,
            Some(DEFAULT_STATEMENTS_QUERY_TEXT_REFRESH)
        );
    }

    #[test]
    fn test_query_text_refresh_can_be_disabled() {
        let config = CollectorConfig::new(25).with_statements_query_text_refresh(None);
        assert_eq!(config.statements.query_text_refresh, None);
    }

    #[test]
    fn test_enabled_collectors_in_order() {
        let config = CollectorConfig::new(25).with_enabled(&[
            "tls".to_string(),
            "default".to_string(),
            "statements".to_string(),
        ]);

        assert_eq!(
            config.enabled_collectors_in_order(),
            vec![
                "default".to_string(),
                "statements".to_string(),
                "tls".to_string()
            ]
        );
    }
}
