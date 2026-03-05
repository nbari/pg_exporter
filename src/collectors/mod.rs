use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::Registry;
use sqlx::PgPool;
use std::collections::HashMap;

#[macro_use]
mod register_macro;

pub trait Collector {
    fn name(&self) -> &'static str;

    /// Register metrics with the prometheus registry
    ///
    /// # Errors
    ///
    /// Returns an error if metric registration fails
    fn register_metrics(&self, registry: &Registry) -> Result<()>;

    // lifetime 'a is needed to tie the future to the lifetime of self and pool
    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>>;

    fn enabled_by_default(&self) -> bool {
        false
    }
}

// Make utils available to all collectors (exclusions, etc.)
pub mod util;

/// Convert i64 to f64 for Prometheus metrics.
///
/// This conversion is safe for `PostgreSQL` metric values because:
/// - Values are typically small (row counts, connections, etc.)
/// - f64 has 52-bit mantissa precision, accurate up to 2^53 (9 quadrillion)
/// - `PostgreSQL` metrics will never realistically exceed this threshold
///
/// # Arguments
/// * `value` - The i64 value to convert
///
/// # Returns
/// The f64 representation of the value
#[inline]
#[allow(clippy::cast_precision_loss)]
const fn i64_to_f64(value: i64) -> f64 {
    value as f64
}

// THIS IS THE ONLY PLACE YOU NEED TO ADD NEW COLLECTORS
register_collectors! {
    default => DefaultCollector,
    vacuum => VacuumCollector,
    activity => ActivityCollector,
    locks => LocksCollector,
    database => DatabaseCollector,
    stat => StatCollector,
    replication => ReplicationCollector,
    index => IndexCollector,
    statements => StatementsCollector,
    exporter => ExporterCollector,
    tls => TlsCollector,
    citus => CitusCollector,
    // Add more collectors here - just follow the same pattern!
}

// Other modules
pub mod config;
pub mod registry;
