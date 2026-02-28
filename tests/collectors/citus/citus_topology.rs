use anyhow::{Result, bail, ensure};
use pg_exporter::collectors::{Collector, citus::CitusCollector};
use prometheus::{Registry, proto::MetricFamily};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{env, path::Path, time::Duration};
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::IntoContainerPort,
    runners::AsyncRunner,
};
use tokio::time::sleep;

const CONNECT_ATTEMPTS: u32 = 60;

fn socket_exists(host: &str) -> bool {
    if let Some(path) = host.strip_prefix("unix://") {
        Path::new(path).exists()
    } else {
        true
    }
}

fn testcontainers_runtime_candidates() -> Vec<String> {
    let mut candidates = vec!["unix:///var/run/docker.sock".to_string()];
    if let Ok(runtime_dir) = env::var("XDG_RUNTIME_DIR")
        && !runtime_dir.is_empty()
    {
        candidates.push(format!("unix://{runtime_dir}/.docker/run/docker.sock"));
    }
    if let Ok(home) = env::var("HOME")
        && !home.is_empty()
    {
        candidates.push(format!("unix://{home}/.docker/run/docker.sock"));
        candidates.push(format!("unix://{home}/.docker/desktop/docker.sock"));
    }
    candidates
}

fn detect_podman_socket() -> Option<String> {
    let mut candidates = vec![
        "unix:///run/podman/podman.sock".to_string(),
        "unix:///var/run/podman/podman.sock".to_string(),
    ];
    if let Ok(runtime_dir) = env::var("XDG_RUNTIME_DIR")
        && !runtime_dir.is_empty()
    {
        candidates.push(format!("unix://{runtime_dir}/podman/podman.sock"));
    }
    if let Ok(uid) = env::var("UID")
        && !uid.is_empty()
    {
        candidates.push(format!("unix:///run/user/{uid}/podman/podman.sock"));
    }

    candidates
        .into_iter()
        .find(|candidate| socket_exists(candidate))
}

fn find_container_runtime() -> Option<String> {
    if let Ok(existing) = env::var("DOCKER_HOST")
        && !existing.is_empty()
        && socket_exists(&existing)
    {
        return Some(existing);
    }

    testcontainers_runtime_candidates()
        .into_iter()
        .find(|candidate| socket_exists(candidate))
}

fn should_require_container_runtime() -> bool {
    let in_ci = env::var("CI")
        .ok()
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let force = env::var("PG_EXPORTER_REQUIRE_TESTCONTAINERS")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE"));

    in_ci || force
}

fn ensure_container_runtime_for_test(test_name: &str) -> Result<bool> {
    if find_container_runtime().is_some() {
        return Ok(true);
    }

    let mut message = format!(
        "No container runtime socket found (checked Podman + Docker), cannot run {test_name}"
    );

    if let Some(podman_socket) = detect_podman_socket() {
        message.push_str(". Podman socket detected at ");
        message.push_str(&podman_socket);
        message.push_str("; set DOCKER_HOST to this value so testcontainers can use it");
    }

    if should_require_container_runtime() {
        bail!("{message}");
    }

    eprintln!("{message}; skipping");
    Ok(false)
}

async fn connect_pool_with_retry(host: &str, port: u16) -> Result<PgPool> {
    let dsn = format!("postgresql://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
    let mut last_error = None;

    for _ in 0..CONNECT_ATTEMPTS {
        match PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(2))
            .connect(&dsn)
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    bail!("failed to connect to {dsn}; last_error={last_error:?}")
}

async fn connect_pool_for_container(container: &ContainerAsync<GenericImage>) -> Result<PgPool> {
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(5432.tcp()).await?;
    connect_pool_with_retry(&host, port).await
}

fn gauge_vec_sample_count(families: &[MetricFamily], metric_name: &str) -> usize {
    families
        .iter()
        .find(|family| family.name() == metric_name)
        .map_or(0, |family| family.get_metric().len())
}

fn int_gauge_value(families: &[MetricFamily], metric_name: &str) -> Option<f64> {
    families
        .iter()
        .find(|family| family.name() == metric_name)
        .and_then(|family| family.get_metric().first())
        .map(|m| m.get_gauge().value())
}

fn has_label(families: &[MetricFamily], metric_name: &str, label_name: &str) -> bool {
    families
        .iter()
        .find(|family| family.name() == metric_name)
        .is_some_and(|family| {
            family
                .get_metric()
                .iter()
                .any(|m| m.get_label().iter().any(|l| l.name() == label_name))
        })
}

async fn start_citus_container(
    image_tag: &str,
) -> Result<ContainerAsync<GenericImage>> {
    GenericImage::new("citusdata/citus", image_tag)
        .with_exposed_port(5432.tcp())
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .start()
        .await
        .map_err(Into::into)
}

async fn setup_citus_test_data(pool: &PgPool) -> Result<()> {
    // Citus extension is already created by the citusdata/citus image,
    // but ensure it exists
    sqlx::query("CREATE EXTENSION IF NOT EXISTS citus")
        .execute(pool)
        .await?;

    // Set coordinator host for single-node mode (use internal container port 5432)
    sqlx::query("SELECT citus_set_coordinator_host('localhost', 5432)")
        .execute(pool)
        .await?;

    // Enable shards on the coordinator so distributed tables can be created
    sqlx::query("SELECT citus_set_node_property('localhost', 5432, 'shouldhaveshards', true)")
        .execute(pool)
        .await?;

    // Create a distributed table
    sqlx::query("CREATE TABLE IF NOT EXISTS test_dist(id int, data text)")
        .execute(pool)
        .await?;
    sqlx::query("SELECT create_distributed_table('test_dist', 'id')")
        .execute(pool)
        .await?;

    // Create a reference table
    sqlx::query("CREATE TABLE IF NOT EXISTS test_ref(id int)")
        .execute(pool)
        .await?;
    sqlx::query("SELECT create_reference_table('test_ref')")
        .execute(pool)
        .await?;

    // Insert sample data
    sqlx::query("INSERT INTO test_dist SELECT g, 'data_' || g FROM generate_series(1, 100) g")
        .execute(pool)
        .await?;
    sqlx::query("INSERT INTO test_ref SELECT g FROM generate_series(1, 10) g")
        .execute(pool)
        .await?;

    Ok(())
}

async fn run_citus_collector_test(image_tag: &str) -> Result<()> {
    let test_name = format!("citus_topology_{image_tag}");
    if !ensure_container_runtime_for_test(&test_name)? {
        return Ok(());
    }

    let require_runtime = should_require_container_runtime();

    let container = match start_citus_container(image_tag).await {
        Ok(container) => container,
        Err(error) => {
            if require_runtime {
                return Err(error);
            }
            eprintln!("Skipping citus topology test ({image_tag}): {error}");
            return Ok(());
        }
    };

    let pool = connect_pool_for_container(&container).await?;

    setup_citus_test_data(&pool).await?;

    // Run the collector
    let registry = Registry::new();
    let collector = CitusCollector::new();
    collector.register_metrics(&registry)?;
    collector.collect(&pool).await?;

    let families = registry.gather();

    // Verify node metrics
    let nodes_total = int_gauge_value(&families, "citus_nodes_total");
    ensure!(
        nodes_total.is_some_and(|v| v >= 1.0),
        "citus_nodes_total should be >= 1, got {nodes_total:?}"
    );

    let active_samples = gauge_vec_sample_count(&families, "citus_node_is_active");
    ensure!(
        active_samples >= 1,
        "citus_node_is_active should have at least 1 sample, got {active_samples}"
    );

    // Verify table metrics
    let tables_total = int_gauge_value(&families, "citus_tables_total");
    ensure!(
        tables_total.is_some_and(|v| v >= 2.0),
        "citus_tables_total should be >= 2 (dist + ref), got {tables_total:?}"
    );

    ensure!(
        has_label(&families, "citus_table_shard_count", "table_name"),
        "citus_table_shard_count should have table_name label"
    );
    ensure!(
        has_label(&families, "citus_table_shard_count", "citus_table_type"),
        "citus_table_shard_count should have citus_table_type label"
    );

    // Verify shard metrics
    let shards_total = int_gauge_value(&families, "citus_shards_total");
    ensure!(
        shards_total.is_some_and(|v| v > 0.0),
        "citus_shards_total should be > 0, got {shards_total:?}"
    );

    let shard_size_samples = gauge_vec_sample_count(&families, "citus_shard_size_bytes");
    ensure!(
        shard_size_samples > 0,
        "citus_shard_size_bytes should have samples"
    );
    ensure!(
        has_label(&families, "citus_shard_size_bytes", "nodename"),
        "citus_shard_size_bytes should have nodename label"
    );
    ensure!(
        has_label(&families, "citus_shard_size_bytes", "nodeport"),
        "citus_shard_size_bytes should have nodeport label"
    );

    // Verify shards_per_node metrics
    let shards_per_node_samples = gauge_vec_sample_count(&families, "citus_shards_per_node");
    ensure!(
        shards_per_node_samples > 0,
        "citus_shards_per_node should have samples"
    );

    // Verify activity metrics (citus_dist_stat_activity is available on both Citus 12+)
    let activity_total = int_gauge_value(&families, "citus_dist_activity_total");
    ensure!(
        activity_total.is_some(),
        "citus_dist_activity_total should exist"
    );

    // Verify stat_counters metrics if available (Citus 12+ has citus_stat_counters,
    // but some sub-versions may not). The collector gracefully skips if the view
    // doesn't exist, so we only verify when samples are present.
    let stat_counter_samples = gauge_vec_sample_count(
        &families,
        "citus_connection_establishment_succeeded_total",
    );
    if stat_counter_samples > 0 {
        ensure!(
            has_label(
                &families,
                "citus_connection_establishment_succeeded_total",
                "database"
            ),
            "citus_connection_establishment_succeeded_total should have database label"
        );
    }

    pool.close().await;

    Ok(())
}

#[tokio::test]
async fn citus_topology_citus_12() -> Result<()> {
    run_citus_collector_test("12.1.3").await
}

#[tokio::test]
async fn citus_topology_citus_13() -> Result<()> {
    run_citus_collector_test("13.0.2").await
}
