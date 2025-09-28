use std::collections::HashSet;

#[derive(Clone, Debug, Default)]
pub struct CollectorConfig {
    pub enabled_collectors: HashSet<String>,
}

impl CollectorConfig {
    /// Create an empty config
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable collectors by name
    pub fn with_enabled(mut self, collectors: &[String]) -> Self {
        self.enabled_collectors = collectors.iter().cloned().collect();
        self
    }

    /// Check if a collector is enabled
    pub fn is_enabled(&self, name: &str) -> bool {
        self.enabled_collectors.contains(name)
    }
}
