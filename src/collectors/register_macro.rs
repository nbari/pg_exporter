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

            async fn collect(&self, pool: &PgPool) -> Result<()> {
                match self {
                    $(
                        CollectorType::$collector_type(c) => c.collect(pool).await,
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

        // Generate the factory function map
        pub fn all_factories() -> HashMap<&'static str, fn() -> CollectorType> {
            let mut map: HashMap<&'static str, fn() -> CollectorType> = HashMap::new();
            $(
                map.insert(
                    stringify!($module),
                    || CollectorType::$collector_type($collector_type::new()),
                );
            )*
            map
        }

        // Generate array of collector names - this is what you need for clap!
        pub const COLLECTOR_NAMES: &[&'static str] = &[
            $(stringify!($module),)*
        ];
    };
}
