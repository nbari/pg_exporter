use anyhow::Result;
use prometheus::Registry;
use sqlx::PgPool;
use std::collections::HashMap;

#[macro_use]
mod register_macro;

pub trait Collector {
    fn name(&self) -> &'static str;

    fn enabled_by_default(&self) -> bool;

    // New method: register metrics with the prometheus registry
    fn register_metrics(&self, registry: &Registry) -> Result<()>;

    // Modified: collect now updates the registered metrics instead of returning strings
    fn collect(&self, pool: &PgPool) -> impl std::future::Future<Output = Result<()>> + Send;
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
