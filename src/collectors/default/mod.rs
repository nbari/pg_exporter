use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::Registry;
use sqlx::PgPool;
use std::sync::Arc;

mod version;
use version::VersionCollector;

#[derive(Clone, Default)]
pub struct DefaultCollector {
    subs: Vec<Arc<dyn Collector + Send + Sync>>,
}

impl DefaultCollector {
    pub fn new() -> Self {
        Self {
            subs: vec![Arc::new(VersionCollector::new())],
        }
    }
}

impl Collector for DefaultCollector {
    fn name(&self) -> &'static str {
        "default"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        for sub in &self.subs {
            sub.register_metrics(registry)?;
        }
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for sub in &self.subs {
                sub.collect(pool).await?;
            }
            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        true
    }
}
