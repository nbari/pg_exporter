use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;

pub mod progress;
pub mod stats;

/// Main Vacuum Collector (aggregates sub-collectors)
#[derive(Clone, Default)]
pub struct VacuumCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl VacuumCollector {
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(stats::VacuumStatsCollector::new()),
                Arc::new(progress::VacuumProgressCollector::new()),
            ],
        }
    }
}

impl Collector for VacuumCollector {
    fn name(&self) -> &'static str {
        "vacuum"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        for sub in &self.subs {
            sub.register_metrics(registry)?;
        }
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        let subs = &self.subs;
        Box::pin(async move {
            for sub in subs {
                sub.collect(pool).await?;
            }
            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
