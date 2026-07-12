use crate::collectors::COLLECTOR_NAMES;
use std::collections::HashSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatementsConfig {
    pub top_n: usize,
}

/// Default minimum `pg_sequences` used-ratio required for a sequence to be exported.
pub const DEFAULT_SEQUENCES_MIN_RATIO: f64 = 0.5;

#[derive(Clone, Debug, PartialEq)]
pub struct SequencesConfig {
    /// Only sequences whose `last_value / max_value` ratio is at least this value are
    /// exported, keeping cardinality bounded so a healthy database exports nothing.
    pub min_ratio: f64,
}

#[derive(Clone, Debug)]
pub struct CollectorConfig {
    pub enabled_collectors: HashSet<String>,
    pub statements: StatementsConfig,
    pub sequences: SequencesConfig,
}

impl CollectorConfig {
    /// Create an empty config with an explicit statements top-N value.
    #[must_use]
    pub fn new(statements_top_n: usize) -> Self {
        Self {
            enabled_collectors: HashSet::new(),
            statements: StatementsConfig {
                top_n: statements_top_n,
            },
            sequences: SequencesConfig {
                min_ratio: DEFAULT_SEQUENCES_MIN_RATIO,
            },
        }
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
