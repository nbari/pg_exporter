use crate::collectors::Collector;
use anyhow::Result;
use futures::future::BoxFuture;
use prometheus::{GaugeVec, Opts, Registry};
use sqlx::{PgPool, Row};
use tracing::info;

/// Tracks PostgreSQL wait events
#[derive(Clone)]
pub struct WaitEventsCollector {
    wait_event_type: GaugeVec,
    wait_event: GaugeVec,
}

impl Default for WaitEventsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitEventsCollector {
    pub fn new() -> Self {
        let wait_event_type = GaugeVec::new(
            Opts::new(
                "pg_wait_event_type",
                "Number of active sessions per wait_event_type",
            ),
            &["type"],
        )
        .expect("Failed to create wait_event_type metric");

        let wait_event = GaugeVec::new(
            Opts::new("pg_wait_event", "Number of active sessions per wait_event"),
            &["event"],
        )
        .expect("Failed to create wait_event metric");

        Self {
            wait_event_type,
            wait_event,
        }
    }
}

impl Collector for WaitEventsCollector {
    fn name(&self) -> &'static str {
        "wait_events"
    }

    fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(self.wait_event_type.clone()))?;
        registry.register(Box::new(self.wait_event.clone()))?;
        Ok(())
    }

    fn collect<'a>(&'a self, pool: &'a PgPool) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Reset metrics first
            self.wait_event_type.reset();
            self.wait_event.reset();

            let rows = sqlx::query(
                r#"
                SELECT
                    wait_event_type,
                    wait_event,
                    count(*) AS count
                FROM pg_stat_activity
                WHERE state = 'active'
                  AND pid != pg_backend_pid()
                GROUP BY wait_event_type, wait_event
                "#,
            )
            .fetch_all(pool)
            .await?;

            if rows.is_empty() {
                // No active waits: set a default "none" value
                self.wait_event_type.with_label_values(&["none"]).set(0.0);
                self.wait_event.with_label_values(&["none"]).set(0.0);
            } else {
                for row in &rows {
                    let event_type: String = row.try_get("wait_event_type")?;
                    let event: String = row.try_get("wait_event")?;
                    let count: i64 = row.try_get("count").unwrap_or(0);

                    self.wait_event_type
                        .with_label_values(&[&event_type])
                        .set(count as f64);
                    self.wait_event
                        .with_label_values(&[&event])
                        .set(count as f64);
                }
            }

            info!("Collected wait events: {}", rows.len());

            Ok(())
        })
    }

    fn enabled_by_default(&self) -> bool {
        false
    }
}
