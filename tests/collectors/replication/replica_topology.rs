use super::super::common;
use anyhow::{Context, Result, bail, ensure};
use pg_exporter::collectors::{
    Collector,
    replication::{
        replica::ReplicaCollector, slots::ReplicationSlotsCollector,
        stat_replication::StatReplicationCollector,
    },
};
use prometheus::{Registry, proto::MetricFamily};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::{env, path::Path, time::Duration};
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{CmdWaitFor, ExecCommand, IntoContainerPort},
    runners::AsyncRunner,
};
use tokio::time::sleep;
use ulid::Ulid;

const POSTGRES_TAG: &str = "16";
const CONNECT_ATTEMPTS: u32 = 60;
const OBSERVE_ATTEMPTS: u32 = 60;
const REPLAY_WAIT_ATTEMPTS: u32 = 80;
const NON_REPLICA_LAG_SENTINEL_SECONDS: f64 = 0.0;

const REPLICA_BOOTSTRAP_SCRIPT: &str = r#"
set -euo pipefail
DATA_DIR="/tmp/pg-replica-data"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"
until pg_basebackup -h "${PRIMARY_HOST}" -D "${DATA_DIR}" -U postgres -Fp -Xs -P -R; do
  sleep 1
done
chmod 700 "${DATA_DIR}"
echo "hot_standby = on" >> "${DATA_DIR}/postgresql.conf"
exec postgres -D "${DATA_DIR}" -c hot_standby=on
"#;

// Keep in sync with postgres_exporter collector/pg_replication.go
const POSTGRES_EXPORTER_REPLICATION_QUERY: &str = r"
SELECT
    CASE
        WHEN NOT pg_is_in_recovery() THEN 0::double precision
        WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0::double precision
        ELSE COALESCE(
            GREATEST(
                0::double precision,
                EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::double precision
            ),
            0::double precision
        )
    END AS lag,
    CASE
        WHEN pg_is_in_recovery() THEN 1
        ELSE 0
    END::bigint AS is_replica,
    COALESCE(
        GREATEST(
            0::double precision,
            EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::double precision
        ),
        0::double precision
    ) AS last_replay
";

#[derive(Debug, Clone, Copy)]
struct ReplicaSnapshot {
    lag_seconds: f64,
    is_replica: i64,
    last_replay_seconds: f64,
}

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

fn approx_equal_seconds(left: f64, right: f64, tolerance: f64) -> bool {
    (left - right).abs() <= tolerance
}

fn gauge_value(families: &[MetricFamily], metric_name: &str) -> Result<f64> {
    let family = families
        .iter()
        .find(|family| family.name() == metric_name)
        .with_context(|| format!("missing metric family: {metric_name}"))?;

    let metric = family
        .get_metric()
        .first()
        .with_context(|| format!("missing metric sample: {metric_name}"))?;

    Ok(metric.get_gauge().value())
}

fn optional_gauge_sample_count(families: &[MetricFamily], metric_name: &str) -> Option<usize> {
    families
        .iter()
        .find(|family| family.name() == metric_name)
        .map(|family| family.get_metric().len())
}

fn ensure_snapshot_matches_query(
    stage: &str,
    current: ReplicaSnapshot,
    expected: ReplicaSnapshot,
) -> Result<()> {
    ensure!(
        current.is_replica == expected.is_replica,
        "is_replica mismatch with postgres_exporter query {stage}: ours={}, expected={}",
        current.is_replica,
        expected.is_replica
    );
    ensure!(
        approx_equal_seconds(current.lag_seconds, expected.lag_seconds, 2.0),
        "lag mismatch with postgres_exporter query {stage}: ours={}, expected={}",
        current.lag_seconds,
        expected.lag_seconds
    );
    ensure!(
        approx_equal_seconds(
            current.last_replay_seconds,
            expected.last_replay_seconds,
            2.0
        ),
        "last_replay mismatch with postgres_exporter query {stage}: ours={}, expected={}",
        current.last_replay_seconds,
        expected.last_replay_seconds
    );
    Ok(())
}

async fn collect_replica_snapshot(pool: &PgPool) -> Result<ReplicaSnapshot> {
    let collector = ReplicaCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    let families = registry.gather();
    Ok(ReplicaSnapshot {
        lag_seconds: gauge_value(&families, "pg_replication_lag_seconds")?,
        is_replica: common::metric_value_to_i64(gauge_value(
            &families,
            "pg_replication_is_replica",
        )?),
        last_replay_seconds: gauge_value(&families, "pg_replication_last_replay_seconds")?,
    })
}

async fn collect_stat_replication_metrics(pool: &PgPool) -> Result<Vec<MetricFamily>> {
    let collector = StatReplicationCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;
    Ok(registry.gather())
}

async fn collect_replication_slots_metrics(pool: &PgPool) -> Result<Vec<MetricFamily>> {
    let collector = ReplicationSlotsCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;
    Ok(registry.gather())
}

async fn wait_for_primary_stat_replication_metrics(
    primary_pool: &PgPool,
) -> Result<Vec<MetricFamily>> {
    let mut last_samples = 0;

    for _ in 0..OBSERVE_ATTEMPTS {
        let metrics = collect_stat_replication_metrics(primary_pool).await?;
        let samples = optional_gauge_sample_count(&metrics, "pg_stat_replication_pg_wal_lsn_diff")
            .unwrap_or(0);
        if samples >= 1 {
            return Ok(metrics);
        }
        last_samples = samples;
        sleep(Duration::from_millis(500)).await;
    }

    bail!(
        "primary never exposed pg_stat_replication metrics with samples; last_samples={last_samples}"
    )
}

async fn query_postgres_exporter_replication(pool: &PgPool) -> Result<ReplicaSnapshot> {
    let row = sqlx::query(POSTGRES_EXPORTER_REPLICATION_QUERY)
        .fetch_one(pool)
        .await?;

    let lag_seconds: f64 = row.try_get("lag")?;
    let is_replica: i64 = row.try_get("is_replica")?;
    let last_replay_seconds: f64 = row.try_get("last_replay")?;

    Ok(ReplicaSnapshot {
        lag_seconds,
        is_replica,
        last_replay_seconds,
    })
}

async fn receive_and_replay_lsn_equal(pool: &PgPool) -> Result<bool> {
    let equal = sqlx::query_scalar::<_, bool>(
        "SELECT COALESCE(pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn(), true)",
    )
    .fetch_one(pool)
    .await?;
    Ok(equal)
}

async fn connect_pool_with_retry(host: &str, port: u16) -> Result<PgPool> {
    let dsn = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
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

async fn start_primary_container(
    network: &str,
    container_name: &str,
) -> Result<ContainerAsync<GenericImage>> {
    GenericImage::new("postgres", POSTGRES_TAG)
        .with_exposed_port(5432.tcp())
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_network(network)
        .with_container_name(container_name)
        .with_cmd(vec![
            "postgres",
            "-c",
            "listen_addresses=*",
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=10",
            "-c",
            "max_replication_slots=10",
            "-c",
            "hot_standby=on",
        ])
        .start()
        .await
        .map_err(Into::into)
}

async fn start_replica_container(
    network: &str,
    container_name: &str,
    primary_name: &str,
) -> Result<ContainerAsync<GenericImage>> {
    GenericImage::new("postgres", POSTGRES_TAG)
        .with_entrypoint("bash")
        .with_exposed_port(5432.tcp())
        .with_env_var("PRIMARY_HOST", primary_name)
        .with_network(network)
        .with_container_name(container_name)
        .with_user("postgres")
        .with_cmd(vec!["-ceu", REPLICA_BOOTSTRAP_SCRIPT])
        .start()
        .await
        .map_err(Into::into)
}

async fn configure_primary_replication_hba(
    primary: &ContainerAsync<GenericImage>,
    primary_pool: &PgPool,
) -> Result<()> {
    let result = primary
        .exec(
            ExecCommand::new([
                "bash",
                "-ceu",
                "printf '%s\n%s\n' \
                 'host replication postgres 0.0.0.0/0 trust' \
                 'host replication postgres ::/0 trust' \
                 >> \"$PGDATA/pg_hba.conf\"",
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await?;

    let exit_code = result.exit_code().await?;
    ensure!(
        exit_code == Some(0),
        "failed to update primary pg_hba.conf for replication: exit_code={exit_code:?}"
    );

    sqlx::query("SELECT pg_reload_conf()")
        .execute(primary_pool)
        .await?;
    Ok(())
}

async fn wait_for_replica_recovery(replica_pool: &PgPool) -> Result<()> {
    for _ in 0..OBSERVE_ATTEMPTS {
        let in_recovery = sqlx::query_scalar::<_, bool>("SELECT pg_is_in_recovery()")
            .fetch_one(replica_pool)
            .await;

        if in_recovery.is_ok_and(std::convert::identity) {
            return Ok(());
        }

        sleep(Duration::from_secs(1)).await;
    }

    bail!("replica did not enter recovery mode")
}

async fn wait_for_primary_replication_stream(primary_pool: &PgPool) -> Result<()> {
    for _ in 0..OBSERVE_ATTEMPTS {
        let stream_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pg_stat_replication")
            .fetch_one(primary_pool)
            .await;

        if stream_count.is_ok_and(|value| value > 0) {
            return Ok(());
        }

        sleep(Duration::from_secs(1)).await;
    }

    bail!("primary did not report any streaming replica in pg_stat_replication")
}

async fn initialize_replication_probe(primary_pool: &PgPool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pg_exporter_replication_probe (
            marker TEXT PRIMARY KEY
        )",
    )
    .execute(primary_pool)
    .await?;
    Ok(())
}

async fn wait_for_marker_replicated(primary_pool: &PgPool, replica_pool: &PgPool) -> Result<()> {
    let marker = Ulid::new().to_string();
    sqlx::query("INSERT INTO pg_exporter_replication_probe(marker) VALUES ($1)")
        .bind(&marker)
        .execute(primary_pool)
        .await?;

    for _ in 0..OBSERVE_ATTEMPTS {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pg_exporter_replication_probe WHERE marker = $1",
        )
        .bind(&marker)
        .fetch_one(replica_pool)
        .await;

        if count.is_ok_and(|value| value > 0) {
            return Ok(());
        }

        sleep(Duration::from_millis(500)).await;
    }

    bail!("replication probe row did not reach replica")
}

async fn wait_for_lsn_sync(replica_pool: &PgPool, attempts: u32) -> Result<()> {
    for _ in 0..attempts {
        if receive_and_replay_lsn_equal(replica_pool).await? {
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }

    bail!("replica did not converge to equal receive/replay LSN")
}

async fn pause_wal_replay(replica_pool: &PgPool) -> Result<()> {
    sqlx::query("SELECT pg_wal_replay_pause()")
        .execute(replica_pool)
        .await?;

    for _ in 0..OBSERVE_ATTEMPTS {
        let paused = sqlx::query_scalar::<_, bool>("SELECT pg_is_wal_replay_paused()")
            .fetch_one(replica_pool)
            .await?;
        if paused {
            return Ok(());
        }
        sleep(Duration::from_millis(200)).await;
    }

    bail!("WAL replay pause did not take effect")
}

async fn resume_wal_replay(replica_pool: &PgPool) -> Result<()> {
    sqlx::query("SELECT pg_wal_replay_resume()")
        .execute(replica_pool)
        .await?;

    for _ in 0..OBSERVE_ATTEMPTS {
        let paused = sqlx::query_scalar::<_, bool>("SELECT pg_is_wal_replay_paused()")
            .fetch_one(replica_pool)
            .await?;
        if !paused {
            return Ok(());
        }
        sleep(Duration::from_millis(200)).await;
    }

    bail!("WAL replay resume did not take effect")
}

async fn generate_wal_burst(primary_pool: &PgPool, rows: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO pg_exporter_replication_probe(marker)
         SELECT md5(random()::text)
         FROM generate_series(1, $1)",
    )
    .bind(rows)
    .execute(primary_pool)
    .await?;
    sqlx::query("SELECT pg_switch_wal()")
        .execute(primary_pool)
        .await?;
    Ok(())
}

async fn wait_for_positive_lag(
    primary_pool: &PgPool,
    replica_pool: &PgPool,
) -> Result<ReplicaSnapshot> {
    let mut last_snapshot = None;

    for _ in 0..OBSERVE_ATTEMPTS {
        generate_wal_burst(primary_pool, 300).await?;

        let snapshot = collect_replica_snapshot(replica_pool).await?;
        let expected = query_postgres_exporter_replication(replica_pool).await?;
        ensure_snapshot_matches_query("while replay paused", snapshot, expected)?;

        let lsn_equal = receive_and_replay_lsn_equal(replica_pool).await?;
        if snapshot.is_replica == 1 && !lsn_equal && snapshot.lag_seconds > 0.0 {
            return Ok(snapshot);
        }

        last_snapshot = Some(snapshot);
        sleep(Duration::from_millis(500)).await;
    }

    bail!("replica never exposed positive lag while replay paused; last_snapshot={last_snapshot:?}")
}

async fn wait_for_zero_lag(replica_pool: &PgPool) -> Result<ReplicaSnapshot> {
    let mut last_snapshot = None;

    for _ in 0..REPLAY_WAIT_ATTEMPTS {
        let snapshot = collect_replica_snapshot(replica_pool).await?;
        let expected = query_postgres_exporter_replication(replica_pool).await?;
        ensure_snapshot_matches_query("after replay resume", snapshot, expected)?;

        let lsn_equal = receive_and_replay_lsn_equal(replica_pool).await?;
        if snapshot.is_replica == 1
            && lsn_equal
            && (snapshot.lag_seconds - 0.0).abs() < f64::EPSILON
        {
            return Ok(snapshot);
        }

        last_snapshot = Some(snapshot);
        sleep(Duration::from_millis(500)).await;
    }

    bail!("replica lag did not recover to zero; last_snapshot={last_snapshot:?}")
}

async fn wait_for_wal_receiver_disconnect(replica_pool: &PgPool) -> Result<()> {
    for _ in 0..OBSERVE_ATTEMPTS {
        let receiver_active =
            sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pg_stat_wal_receiver)")
                .fetch_one(replica_pool)
                .await?;
        if !receiver_active {
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }

    bail!("replica wal receiver stayed active after primary shutdown")
}

async fn bootstrap_replication_topology(
    primary_pool: &PgPool,
    replica_pool: &PgPool,
) -> Result<()> {
    wait_for_replica_recovery(replica_pool).await?;
    wait_for_primary_replication_stream(primary_pool).await?;
    initialize_replication_probe(primary_pool).await?;
    wait_for_marker_replicated(primary_pool, replica_pool).await?;
    wait_for_lsn_sync(replica_pool, OBSERVE_ATTEMPTS).await
}

async fn assert_primary_role_and_dependent_collectors(primary_pool: &PgPool) -> Result<()> {
    let primary_snapshot = collect_replica_snapshot(primary_pool).await?;
    ensure!(
        primary_snapshot.is_replica == 0,
        "primary should report is_replica=0, got {}",
        primary_snapshot.is_replica
    );
    ensure!(
        approx_equal_seconds(
            primary_snapshot.lag_seconds,
            NON_REPLICA_LAG_SENTINEL_SECONDS,
            f64::EPSILON
        ),
        "primary lag sentinel mismatch: expected {}, got {}",
        NON_REPLICA_LAG_SENTINEL_SECONDS,
        primary_snapshot.lag_seconds
    );
    ensure!(
        primary_snapshot.last_replay_seconds >= 0.0,
        "primary last replay should be non-negative, got {}",
        primary_snapshot.last_replay_seconds
    );
    ensure_snapshot_matches_query(
        "primary role semantics",
        primary_snapshot,
        query_postgres_exporter_replication(primary_pool).await?,
    )?;

    let primary_stat_metrics = wait_for_primary_stat_replication_metrics(primary_pool).await?;
    ensure!(
        optional_gauge_sample_count(&primary_stat_metrics, "pg_stat_replication_pg_wal_lsn_diff")
            .unwrap_or(0)
            >= 1,
        "primary should expose pg_stat_replication rows while replica is connected"
    );
    ensure!(
        optional_gauge_sample_count(
            &primary_stat_metrics,
            "pg_stat_replication_pg_current_wal_lsn_bytes"
        )
        .unwrap_or(0)
            >= 1,
        "primary should expose current WAL LSN bytes metrics"
    );
    ensure!(
        optional_gauge_sample_count(&primary_stat_metrics, "pg_stat_replication_reply_time")
            .unwrap_or(0)
            >= 1,
        "primary should expose reply time metrics for connected replicas"
    );
    ensure!(
        optional_gauge_sample_count(&primary_stat_metrics, "pg_stat_replication_slots")
            .unwrap_or(0)
            >= 1,
        "primary should expose slot-count metric entries while replica is connected"
    );

    let primary_slots_metrics = collect_replication_slots_metrics(primary_pool).await?;
    if let Some(samples) =
        optional_gauge_sample_count(&primary_slots_metrics, "pg_replication_slots_active")
    {
        ensure!(
            samples >= 1,
            "primary replication slots metric family exists but has no samples"
        );
    }

    Ok(())
}

async fn assert_replica_role_and_dependent_collectors(replica_pool: &PgPool) -> Result<()> {
    let replica_snapshot = collect_replica_snapshot(replica_pool).await?;
    ensure!(
        replica_snapshot.is_replica == 1,
        "replica should report is_replica=1, got {}",
        replica_snapshot.is_replica
    );
    ensure!(
        replica_snapshot.lag_seconds >= 0.0,
        "replica lag should be non-negative before backlog scenario, got {}",
        replica_snapshot.lag_seconds
    );
    ensure_snapshot_matches_query(
        "replica baseline semantics",
        replica_snapshot,
        query_postgres_exporter_replication(replica_pool).await?,
    )?;

    let replica_stat_metrics = collect_stat_replication_metrics(replica_pool).await?;
    ensure!(
        optional_gauge_sample_count(&replica_stat_metrics, "pg_stat_replication_pg_wal_lsn_diff")
            .unwrap_or(0)
            == 0,
        "replica should not expose pg_stat_replication rows from primary perspective"
    );

    let replica_slots_metrics = collect_replication_slots_metrics(replica_pool).await?;
    if let Some(samples) =
        optional_gauge_sample_count(&replica_slots_metrics, "pg_replication_slots_active")
    {
        ensure!(
            samples >= 1,
            "replica replication slots metric family exists but has no samples"
        );
    }

    Ok(())
}

async fn assert_backlog_and_catchup_lag_semantics(
    primary_pool: &PgPool,
    replica_pool: &PgPool,
) -> Result<()> {
    pause_wal_replay(replica_pool).await?;
    let backlog_snapshot = wait_for_positive_lag(primary_pool, replica_pool).await?;
    ensure!(
        backlog_snapshot.lag_seconds > 0.0,
        "replica backlog scenario should expose lag > 0, got {}",
        backlog_snapshot.lag_seconds
    );

    resume_wal_replay(replica_pool).await?;
    wait_for_lsn_sync(replica_pool, REPLAY_WAIT_ATTEMPTS).await?;
    let caught_up_snapshot = wait_for_zero_lag(replica_pool).await?;
    ensure!(
        (caught_up_snapshot.lag_seconds - 0.0).abs() < f64::EPSILON,
        "replica catch-up scenario should expose lag = 0, got {}",
        caught_up_snapshot.lag_seconds
    );

    Ok(())
}

async fn assert_broken_and_error_semantics(
    primary: ContainerAsync<GenericImage>,
    replica: ContainerAsync<GenericImage>,
    replica_pool: &PgPool,
) -> Result<()> {
    drop(primary);
    wait_for_wal_receiver_disconnect(replica_pool).await?;

    let broken_snapshot = collect_replica_snapshot(replica_pool).await?;
    ensure!(
        broken_snapshot.is_replica == 1,
        "broken upstream path should still identify node as replica, got {}",
        broken_snapshot.is_replica
    );
    ensure!(
        broken_snapshot.lag_seconds >= 0.0,
        "broken upstream path should keep lag metric non-negative sentinel/unknown semantics, got {}",
        broken_snapshot.lag_seconds
    );
    ensure_snapshot_matches_query(
        "broken upstream path semantics",
        broken_snapshot,
        query_postgres_exporter_replication(replica_pool).await?,
    )?;

    drop(replica);
    sleep(Duration::from_secs(1)).await;

    let collector = ReplicaCollector::new();
    let registry = Registry::new();
    collector.register_metrics(&registry)?;
    let collect_result = collector.collect(replica_pool).await;
    ensure!(
        collect_result.is_err(),
        "collector should return an error when replication target is unreachable"
    );

    Ok(())
}

#[tokio::test]
async fn replication_lag_and_role_semantics_from_postgres_primary_replica_pair() -> Result<()> {
    let test_name = "replication_lag_and_role_semantics_from_postgres_primary_replica_pair";
    if !ensure_container_runtime_for_test(test_name)? {
        return Ok(());
    }

    let require_runtime = should_require_container_runtime();
    let suffix = Ulid::new().to_string().to_lowercase();
    let network = format!("pg-exporter-repl-{suffix}");
    let primary_name = format!("pg-exporter-primary-{suffix}");
    let replica_name = format!("pg-exporter-replica-{suffix}");

    let primary = match start_primary_container(&network, &primary_name).await {
        Ok(container) => container,
        Err(error) => {
            if require_runtime {
                return Err(error);
            }
            eprintln!("Skipping replication topology test: {error}");
            return Ok(());
        }
    };

    let primary_pool = connect_pool_for_container(&primary).await?;
    configure_primary_replication_hba(&primary, &primary_pool).await?;

    let replica = match start_replica_container(&network, &replica_name, &primary_name).await {
        Ok(container) => container,
        Err(error) => {
            if require_runtime {
                return Err(error);
            }
            eprintln!("Skipping replication topology test: {error}");
            return Ok(());
        }
    };

    let replica_pool = connect_pool_for_container(&replica).await?;

    bootstrap_replication_topology(&primary_pool, &replica_pool).await?;
    assert_primary_role_and_dependent_collectors(&primary_pool).await?;
    assert_replica_role_and_dependent_collectors(&replica_pool).await?;
    assert_backlog_and_catchup_lag_semantics(&primary_pool, &replica_pool).await?;
    assert_broken_and_error_semantics(primary, replica, &replica_pool).await?;

    primary_pool.close().await;
    replica_pool.close().await;

    Ok(())
}
