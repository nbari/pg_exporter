macro_rules! register_collectors {
    (
        $(
            $module:ident => $collector_type:ident
        ),* $(,)?
    ) => {
        // Import all collector modules
        $(
            pub mod $module;
            pub use $module::$collector_type;
        )*

        // Generate the enum with all collector types
        #[derive(Clone)]
        pub enum CollectorType {
            $(
                $collector_type($collector_type),
            )*
        }

        // Implement Collector trait for CollectorType enum
        impl Collector for CollectorType {
            fn name(&self) -> &'static str {
                match self {
                    $(
                        CollectorType::$collector_type(c) => c.name(),
                    )*
                }
            }

            fn register_metrics(&self, registry: &Registry) -> Result<()> {
                match self {
                    $(
                        CollectorType::$collector_type(c) => c.register_metrics(registry),
                    )*
                }
            }

            fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
                match self {
                    $(
                        CollectorType::$collector_type(c) => c.collect(pool),
                    )*
                }
            }

            fn enabled_by_default(&self) -> bool {
                match self {
                    $(
                        CollectorType::$collector_type(c) => c.enabled_by_default(),
                    )*
                }
            }
        }

        /// Methods specific to particular collector variants.
        ///
        /// These methods provide capabilities that only certain collectors have,
        /// without polluting the core `Collector` trait with optional methods.
        impl CollectorType {
            /// Get the scraper collector for tracking scrape performance metrics.
            ///
            /// # Design Rationale
            ///
            /// Only `ExporterCollector` tracks scrape performance (duration, errors, etc).
            /// Rather than adding an optional method to the `Collector` trait that 99%
            /// of collectors would return `None` for, we implement this as a method on
            /// the `CollectorType` enum.
            ///
            /// This keeps the trait focused on the universal collector contract while
            /// providing type-safe access to collector-specific capabilities.
            ///
            /// # Returns
            ///
            /// - `Some(Arc<ScraperCollector>)` if this is an `ExporterCollector`
            /// - `None` for all other collector types
            ///
            /// # Example
            ///
            /// ```rust,ignore
            /// // In CollectorRegistry::new()
            /// for (name, factory) in factories {
            ///     let collector = factory();
            ///
            ///     // Extract scraper if exporter collector is enabled
            ///     if let Some(scraper) = collector.get_scraper() {
            ///         // Use scraper to track performance of all collectors
            ///         self.scraper = Some(scraper);
            ///     }
            /// }
            /// ```
            #[must_use] pub fn get_scraper(&self) -> Option<std::sync::Arc<crate::collectors::exporter::ScraperCollector>> {
                match self {
                    // ExporterCollector is the only collector that tracks scrape performance
                    CollectorType::ExporterCollector(c) => Some(c.get_scraper().clone()),
                    // All other collectors don't have scraping capabilities
                    _ => None,
                }
            }
        }

        // Generate the factory function map
        #[must_use] pub fn all_factories() -> HashMap<&'static str, fn() -> CollectorType> {
            let mut map: HashMap<&'static str, fn() -> CollectorType> = HashMap::new();
            $(
                map.insert(
                    stringify!($module),
                    || CollectorType::$collector_type($collector_type::new()),
                );
            )*
            map
        }

        // Generate array of collector names
        pub const COLLECTOR_NAMES: &[&'static str] = &[
            $(stringify!($module),)*
        ];
    };
}

#[cfg(test)]
mod tests {
    use crate::collectors::Collector;
    use prometheus::Registry;

    // Test that the macro works with the actual collectors in the parent module
    #[test]
    fn test_all_factories_exist() {
        let factories = crate::collectors::all_factories();

        // Should have all registered collectors
        assert!(!factories.is_empty());
        assert!(factories.len() >= 5); // At least default, vacuum, activity, locks, database, stat
    }

    #[test]
    fn test_collector_names_exist() {
        let names = crate::collectors::COLLECTOR_NAMES;

        assert!(!names.is_empty());
        assert!(names.len() >= 5);

        // Check expected collectors are present
        assert!(names.contains(&"default"));
        assert!(names.contains(&"vacuum"));
        assert!(names.contains(&"activity"));
        assert!(names.contains(&"locks"));
        assert!(names.contains(&"database"));
        assert!(names.contains(&"stat"));
    }

    #[test]
    fn test_factory_creates_valid_collectors() {
        let factories = crate::collectors::all_factories();

        // Test creating each collector
        for (name, factory) in &factories {
            let collector = factory();

            // Each collector should have a non-empty name
            assert!(
                !collector.name().is_empty(),
                "Collector {name} has empty name"
            );
        }
    }

    #[test]
    fn test_factories_match_collector_names() {
        let factories = crate::collectors::all_factories();
        let names = crate::collectors::COLLECTOR_NAMES;

        // Every factory key should be in COLLECTOR_NAMES
        for key in factories.keys() {
            assert!(
                names.contains(key),
                "Factory key '{key}' not found in COLLECTOR_NAMES"
            );
        }

        // Every name in COLLECTOR_NAMES should have a factory
        for name in names {
            assert!(
                factories.contains_key(name),
                "Name '{name}' in COLLECTOR_NAMES has no factory"
            );
        }

        // Counts should match
        assert_eq!(factories.len(), names.len());
    }

    #[test]
    fn test_collector_name_matches_key() {
        let factories = crate::collectors::all_factories();

        // The collector's name() should match the factory key
        for (key, factory) in &factories {
            let collector = factory();
            assert_eq!(
                collector.name(),
                *key,
                "Collector name '{}' doesn't match factory key '{}'",
                collector.name(),
                key
            );
        }
    }

    #[test]
    fn test_default_collector_enabled_by_default() {
        let factories = crate::collectors::all_factories();

        if let Some(factory) = factories.get("default") {
            let collector = factory();
            assert!(
                collector.enabled_by_default(),
                "Default collector should be enabled by default"
            );
        }
    }

    #[test]
    fn test_register_metrics_does_not_panic() {
        let factories = crate::collectors::all_factories();
        let registry = Registry::new();

        // Test that each collector can register metrics without panicking
        for (name, factory) in &factories {
            let collector = factory();
            let result = collector.register_metrics(&registry);
            assert!(
                result.is_ok(),
                "Collector '{name}' failed to register metrics: {result:?}"
            );
        }
    }

    #[test]
    fn test_collector_names_are_lowercase() {
        let names = crate::collectors::COLLECTOR_NAMES;

        // Convention: collector names should be lowercase
        for name in names {
            assert_eq!(
                *name,
                name.to_lowercase(),
                "Collector name '{name}' is not lowercase"
            );
        }
    }

    #[test]
    fn test_collector_names_are_unique() {
        let names = crate::collectors::COLLECTOR_NAMES;
        let mut seen = std::collections::HashSet::new();

        for name in names {
            assert!(
                seen.insert(name),
                "Duplicate collector name found: '{name}'"
            );
        }
    }

    #[test]
    fn test_factory_map_keys_are_unique() {
        let factories = crate::collectors::all_factories();

        // HashMap keys are inherently unique, but let's verify count
        let unique_count = factories
            .keys()
            .collect::<std::collections::HashSet<_>>()
            .len();

        assert_eq!(
            factories.len(),
            unique_count,
            "Factory map has duplicate keys"
        );
    }

    #[test]
    fn test_exporter_collector_is_registered() {
        let names = crate::collectors::COLLECTOR_NAMES;
        assert!(
            names.contains(&"exporter"),
            "Exporter collector should be registered"
        );
    }

    #[test]
    fn test_exporter_collector_factory_exists() {
        let factories = crate::collectors::all_factories();
        assert!(
            factories.contains_key("exporter"),
            "Exporter factory should exist"
        );
    }

    #[test]
    #[allow(clippy::panic)]
    fn test_get_scraper_returns_some_for_exporter() {
        let factories = crate::collectors::all_factories();

        if let Some(factory) = factories.get("exporter") {
            let collector = factory();
            let scraper = collector.get_scraper();

            assert!(
                scraper.is_some(),
                "ExporterCollector should provide a scraper"
            );
        } else {
            panic!("Exporter factory not found");
        }
    }

    #[test]
    fn test_get_scraper_returns_none_for_other_collectors() {
        let factories = crate::collectors::all_factories();

        // Test that non-exporter collectors return None
        let non_exporter_collectors = vec!["default", "vacuum", "activity", "locks"];

        for name in non_exporter_collectors {
            if let Some(factory) = factories.get(name) {
                let collector = factory();
                let scraper = collector.get_scraper();

                assert!(
                    scraper.is_none(),
                    "Collector '{name}' should not provide a scraper"
                );
            }
        }
    }

    #[test]
    fn test_exporter_collector_name_matches() {
        let factories = crate::collectors::all_factories();

        if let Some(factory) = factories.get("exporter") {
            let collector = factory();
            assert_eq!(
                collector.name(),
                "exporter",
                "ExporterCollector name should be 'exporter'"
            );
        }
    }
}
