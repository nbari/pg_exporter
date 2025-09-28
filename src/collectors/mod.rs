use anyhow::Result;
use sqlx::PgPool;
use std::collections::HashMap;

#[macro_use]
mod register_macro;

// The trait defines the interface - this is idiomatic Rust
pub trait Collector {
    fn name(&self) -> &'static str;
    fn collect(&self, pool: &PgPool) -> impl std::future::Future<Output = Result<String>> + Send;
    fn enabled_by_default(&self) -> bool {
        true
    }
}

pub struct CollectorSpec {
    pub name: &'static str,
}

// THIS IS THE ONLY PLACE YOU NEED TO ADD NEW COLLECTORS ✨
register_collectors! {
    default => DefaultCollector,
    vacuum => VacuumCollector
    // Add more collectors here - just follow the same pattern!
}

// Other modules
pub mod config;
pub mod registry;
