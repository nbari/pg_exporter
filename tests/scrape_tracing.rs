//! Issue #39: exercise the registry and database task boundaries in a separate
//! process, isolating the connection configuration and tracing subscriber.
use anyhow::{Result, anyhow, ensure};
use pg_exporter::collectors::{config::CollectorConfig, registry::CollectorRegistry, util};
use std::sync::mpsc::Receiver;
use tracing::{Instrument as _, info_span, span::Id};

mod common;
#[path = "support/trace_capture.rs"]
mod trace_capture;
use trace_capture::Observation;

#[tokio::test(flavor = "current_thread")]
async fn database_query_spans_retain_the_scrape_request() -> Result<()> {
    let (subscriber, observations) = trace_capture::subscriber();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let admin = common::create_test_pool().await?;
    let db = common::IsolatedTestDatabase::new("scrape_tracing").await?;
    // Use Result assertions so cleanup is awaited on both success and failure.
    let result = verify_scrapes(&admin, &db, &observations).await;
    let cleanup = db.cleanup().await;
    admin.close().await;
    result.and(cleanup)
}

async fn verify_scrapes(
    admin: &sqlx::PgPool,
    db: &common::IsolatedTestDatabase,
    observations: &Receiver<Observation>,
) -> Result<()> {
    for statement in [
        "CREATE TABLE trace_items (id bigint PRIMARY KEY)",
        "INSERT INTO trace_items VALUES (1)",
        "CREATE SEQUENCE trace_sequence MAXVALUE 100",
        "SELECT setval('trace_sequence', 75)",
    ] {
        sqlx::query(statement).execute(db.pool()).await?;
    }
    let default_db =
        util::get_default_database().ok_or_else(|| anyhow!("default database not initialized"))?;
    let config =
        CollectorConfig::new(10).with_enabled(&["stat".into(), "sequences".into(), "index".into()]);
    let registry = CollectorRegistry::new(&config);
    for request_id in ["first_scrape", "second_scrape"] {
        // Discard setup observations before each independent request.
        for _ in observations.try_iter() {}
        let root =
            info_span!(parent: None, "http.server.request", request_id, http.route = "/metrics");
        let root_id = root.id().ok_or_else(|| anyhow!("request span disabled"))?;
        let metrics = registry.collect_all(admin).instrument(root).await?;
        for metric in [
            "pg_stat_user_tables_seq_scan",
            "pg_sequence_used_ratio",
            "pg_index_scans_total",
        ] {
            ensure!(
                metrics
                    .lines()
                    .any(|line| line.starts_with(metric) && line.contains(db.database_name())),
                "fixture metric {metric} missing"
            );
        }
        let records: Vec<_> = observations.try_iter().collect();
        assert_spans(
            &records,
            &root_id,
            request_id,
            default_db,
            db.database_name(),
        )?;
    }
    Ok(())
}

fn assert_spans(
    records: &[Observation],
    root_id: &Id,
    request_id: &str,
    default_db: &str,
    fixture_db: &str,
) -> Result<()> {
    for name in ["db.connectivity_check", "collector.collect"] {
        let mut matching = records
            .iter()
            .filter(|record| record.name == name)
            .peekable();
        ensure!(matching.peek().is_some(), "{name} was never observed");
        for record in matching {
            trace_capture::assert_request(record, root_id, request_id)?;
        }
    }
    for (table, collector) in [
        ("pg_stat_user_tables", "stat"),
        ("pg_sequences", "sequences"),
        ("pg_stat_user_indexes", "index"),
    ] {
        for datname in [default_db, fixture_db] {
            let query = records
                .iter()
                .find(|record| {
                    record.name == "db.query"
                        && record.fields.get("db.sql.table").map(String::as_str) == Some(table)
                        && record.fields.get("datname").map(String::as_str) == Some(datname)
                })
                .ok_or_else(|| anyhow!("no {table} query span for {datname}"))?;
            trace_capture::assert_request(query, root_id, request_id)?;
            ensure!(
                query
                    .scope
                    .iter()
                    .any(|span| span.name == "collector.collect"
                        && span.fields.get("collector").map(String::as_str) == Some(collector)),
                "{table} query lost its top-level collector: {query:?}"
            );
            if collector != "index" {
                let reuse = if datname == default_db {
                    "true"
                } else {
                    "false"
                };
                ensure!(
                    query.fields.get("reuse_pool").map(String::as_str) == Some(reuse),
                    "wrong connection path for {datname}"
                );
            }
        }
    }
    Ok(())
}
