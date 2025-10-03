use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;

mod connections;
use connections::ConnectionsCollector;

mod locks;
use locks::LocksCollector;

/// Main Activity Collector (aggregates sub-collectors)
#[derive(Clone, Default)]
pub struct ActivityCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl ActivityCollector {
    pub fn new() -> Self {
        Self {
            subs: vec![
                Arc::new(ConnectionsCollector::new()),
                Arc::new(LocksCollector::new()),
            ],
        }
    }
}

impl Collector for ActivityCollector {
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
        false
    }
}
