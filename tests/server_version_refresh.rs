//! Regression for a `PostgreSQL` server upgraded under a running exporter. Separate
//! process: the test seeds the process-wide server-version cache with a wrong value,
//! which would race the version-gated collectors of any other test sharing the process.
use anyhow::{Context, Result, ensure};
use pg_exporter::collectors::{
    config::CollectorConfig,
    registry::CollectorRegistry,
    util::{get_pg_version, refresh_pg_version},
};

mod common;

/// The exporter outlives the server it monitors: an in-place major upgrade, or a newer
/// server taking over the same address, only shows up as a few scrapes of `pg_up 0`.
/// The cached version used to be written once per process, so collectors that choose
/// their SQL by version kept using the old server's. After a 17 to 18 upgrade `stat_io`
/// selected `op_bytes`, which no longer exists, and every scrape failed until the
/// exporter was restarted.
#[tokio::test]
async fn scrape_follows_the_server_after_a_major_version_change() -> Result<()> {
    let pool = common::create_test_pool().await?;
    let live: i32 = sqlx::query_scalar::<_, String>("SHOW server_version_num")
        .fetch_one(&pool)
        .await?
        .parse()
        .context("server_version_num is not a number")?;

    // What a long-running exporter still holds after the upgrade: another major's version.
    // Either choice makes `stat_io` pick SQL that this server rejects.
    let stale = if live >= 180_000 { 170_000 } else { 180_000 };
    let _ = refresh_pg_version(stale);
    ensure!(
        get_pg_version() == stale,
        "could not seed a stale server version"
    );

    let enabled = ["default", "stat_io"].map(str::to_string);
    let registry = CollectorRegistry::new(&CollectorConfig::new(25).with_enabled(&enabled));

    let body = registry
        .collect_all(&pool)
        .await
        .context("a scrape must not fail because the cached server version is stale")?;

    ensure!(
        get_pg_version() == live,
        "the scrape left the cached server version at {} while the server reports {live}",
        get_pg_version()
    );
    ensure!(
        body.contains("pg_up 1"),
        "the scrape did not report the server as up"
    );

    pool.close().await;
    Ok(())
}
