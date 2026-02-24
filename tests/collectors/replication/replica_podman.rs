use super::super::common;
use anyhow::{Context, Result, bail};
use pg_exporter::collectors::{Collector, replication::replica::ReplicaCollector};
use prometheus::Registry;
use sqlx::{PgPool, Row};
use std::{
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};
use tokio::time::{Duration, sleep};

// Keep in sync with postgres_exporter collector/pg_replication.go
const POSTGRES_EXPORTER_REPLICATION_QUERY: &str = r"
SELECT
    CASE
        WHEN NOT pg_is_in_recovery() THEN 0
        WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
        ELSE GREATEST(0, EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())))
    END::double precision AS lag,
    CASE
        WHEN pg_is_in_recovery() THEN 1
        ELSE 0
    END::bigint AS is_replica,
    GREATEST(0, EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())))::double precision AS last_replay
";

#[derive(Debug, Clone, Copy)]
struct ReplicaSnapshot {
    lag_seconds: f64,
    is_replica: i64,
    last_replay_seconds: f64,
}

#[derive(Clone)]
struct PodmanTopology {
    primary_port: u16,
    replica_port: u16,
    network_name: String,
    primary_container: String,
    replica_container: String,
    pg_version: String,
}

impl PodmanTopology {
    fn new() -> Self {
        let primary_port = common::get_available_port();
        let mut replica_port = common::get_available_port();
        if replica_port == primary_port {
            replica_port = common::get_available_port();
        }

        let suffix = format!("{}-{primary_port}", std::process::id());

        Self {
            primary_port,
            replica_port,
            network_name: format!("pg-exporter-repl-net-{suffix}"),
            primary_container: format!("pg-exporter-repl-primary-{suffix}"),
            replica_container: format!("pg-exporter-repl-replica-{suffix}"),
            pg_version: "16".to_string(),
        }
    }
}

struct PodmanTopologyGuard {
    manifest_dir: PathBuf,
    topology: PodmanTopology,
}

impl Drop for PodmanTopologyGuard {
    fn drop(&mut self) {
        let stop_script = self.manifest_dir.join("tests/stop-replication-postgres.sh");

        let mut command = Command::new("bash");
        command.arg(stop_script);
        apply_topology_env(&mut command, &self.topology);

        match command.output() {
            Ok(output) if output.status.success() => {}
            Ok(output) => {
                eprintln!(
                    "failed to stop podman replication topology: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            Err(error) => {
                eprintln!("failed to execute stop script: {error}");
            }
        }
    }
}

fn apply_topology_env(command: &mut Command, topology: &PodmanTopology) {
    command
        .env("PRIMARY_PORT", topology.primary_port.to_string())
        .env("REPLICA_PORT", topology.replica_port.to_string())
        .env("NETWORK_NAME", &topology.network_name)
        .env("PRIMARY_CONTAINER", &topology.primary_container)
        .env("REPLICA_CONTAINER", &topology.replica_container)
        .env("PG_VERSION", &topology.pg_version);
}

fn run_script(script: &Path, topology: &PodmanTopology) -> Result<Output> {
    let mut command = Command::new("bash");
    command.arg(script);
    apply_topology_env(&mut command, topology);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());

    let output = command
        .output()
        .with_context(|| format!("failed to execute {}", script.display()))?;

    Ok(output)
}

fn approx_equal_seconds(left: f64, right: f64, tolerance: f64) -> bool {
    (left - right).abs() <= tolerance
}

async fn collect_replica_metrics(pool: &PgPool) -> Result<ReplicaSnapshot> {
    let registry = Registry::new();
    let collector = ReplicaCollector::new();

    collector.register_metrics(&registry)?;
    collector.collect(pool).await?;

    let mut lag_seconds = None;
    let mut is_replica = None;
    let mut last_replay_seconds = None;

    for family in registry.gather() {
        if family.name() == "pg_replication_lag_seconds"
            && let Some(metric) = family.get_metric().first()
        {
            lag_seconds = Some(metric.get_gauge().value());
        }

        if family.name() == "pg_replication_is_replica"
            && let Some(metric) = family.get_metric().first()
        {
            is_replica = Some(common::metric_value_to_i64(metric.get_gauge().value()));
        }

        if family.name() == "pg_replication_last_replay_seconds"
            && let Some(metric) = family.get_metric().first()
        {
            last_replay_seconds = Some(metric.get_gauge().value());
        }
    }

    Ok(ReplicaSnapshot {
        lag_seconds: lag_seconds.context("missing pg_replication_lag_seconds")?,
        is_replica: is_replica.context("missing pg_replication_is_replica")?,
        last_replay_seconds: last_replay_seconds
            .context("missing pg_replication_last_replay_seconds")?,
    })
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

async fn start_replication_topology() -> Result<(PodmanTopologyGuard, PgPool, PgPool)> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let start_script = manifest_dir.join("tests/start-replication-postgres.sh");
    let topology = PodmanTopology::new();

    let start_output = run_script(&start_script, &topology)?;
    if !start_output.status.success() {
        bail!(
            "failed to start replication topology\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&start_output.stdout),
            String::from_utf8_lossy(&start_output.stderr)
        );
    }

    let guard = PodmanTopologyGuard {
        manifest_dir,
        topology: topology.clone(),
    };

    let primary_dsn = format!(
        "postgresql://postgres:postgres@localhost:{}/postgres",
        topology.primary_port
    );
    let replica_dsn = format!(
        "postgresql://postgres:postgres@localhost:{}/postgres",
        topology.replica_port
    );

    let primary_pool = PgPool::connect(&primary_dsn).await?;
    let replica_pool = PgPool::connect(&replica_dsn).await?;

    Ok((guard, primary_pool, replica_pool))
}

async fn initialize_replication_lag_test_table(primary_pool: &PgPool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pg_exporter_replica_lag_test (
            id BIGSERIAL PRIMARY KEY,
            note TEXT NOT NULL
        )",
    )
    .execute(primary_pool)
    .await?;

    sqlx::query("INSERT INTO pg_exporter_replica_lag_test(note) VALUES ('baseline')")
        .execute(primary_pool)
        .await?;

    Ok(())
}

async fn wait_until_baseline_replayed(replica_pool: &PgPool) -> Result<()> {
    for _ in 0..40 {
        let replayed_rows = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pg_exporter_replica_lag_test WHERE note = 'baseline'",
        )
        .fetch_one(replica_pool)
        .await;

        if replayed_rows.is_ok_and(|count| count > 0) {
            return Ok(());
        }

        sleep(Duration::from_millis(500)).await;
    }

    bail!("replica never caught up with baseline transaction");
}

fn assert_snapshot_matches_postgres_exporter(
    stage: &str,
    current: ReplicaSnapshot,
    expected: ReplicaSnapshot,
) {
    assert_eq!(
        current.is_replica, expected.is_replica,
        "is_replica mismatch with postgres_exporter query {stage}"
    );

    assert!(
        approx_equal_seconds(current.lag_seconds, expected.lag_seconds, 2.0),
        "lag mismatch with postgres_exporter query {stage}: ours={}, expected={}",
        current.lag_seconds,
        expected.lag_seconds
    );

    assert!(
        approx_equal_seconds(
            current.last_replay_seconds,
            expected.last_replay_seconds,
            2.0
        ),
        "last_replay mismatch with postgres_exporter query {stage}: ours={}, expected={}",
        current.last_replay_seconds,
        expected.last_replay_seconds
    );
}

async fn assert_initial_replica_snapshot(replica_pool: &PgPool) -> Result<()> {
    let initial = collect_replica_metrics(replica_pool).await?;
    let initial_expected = query_postgres_exporter_replication(replica_pool).await?;
    assert_eq!(initial.is_replica, 1, "target must be a replica");
    assert!(
        initial.lag_seconds >= 0.0,
        "lag must be non-negative before replay pause, got {}",
        initial.lag_seconds
    );
    assert_snapshot_matches_postgres_exporter("before replay pause", initial, initial_expected);
    Ok(())
}

async fn pause_replay(replica_pool: &PgPool) -> Result<()> {
    sqlx::query("SELECT pg_wal_replay_pause()")
        .execute(replica_pool)
        .await?;

    let replay_paused = sqlx::query_scalar::<_, bool>("SELECT pg_is_wal_replay_paused()")
        .fetch_one(replica_pool)
        .await?;
    assert!(replay_paused, "failed to pause WAL replay on replica");
    Ok(())
}

async fn generate_wal_burst(primary_pool: &PgPool, rows: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO pg_exporter_replica_lag_test(note)
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

async fn observe_positive_lag_with_parity(
    primary_pool: &PgPool,
    replica_pool: &PgPool,
) -> Result<(bool, bool, f64)> {
    let mut observed_positive_lag = false;
    let mut observed_lsn_divergence = false;
    let mut last_lag = 0.0;

    for _ in 0..60 {
        // Keep generating WAL while replay is paused to make lag visible.
        generate_wal_burst(primary_pool, 300).await?;

        let current = collect_replica_metrics(replica_pool).await?;
        let expected = query_postgres_exporter_replication(replica_pool).await?;
        let lsn_equal = receive_and_replay_lsn_equal(replica_pool).await?;
        if !lsn_equal {
            observed_lsn_divergence = true;
        }

        last_lag = current.lag_seconds;

        assert_snapshot_matches_postgres_exporter("while replay paused", current, expected);

        if current.is_replica == 1 && !lsn_equal && current.lag_seconds > 0.0 {
            observed_positive_lag = true;
            break;
        }

        sleep(Duration::from_millis(500)).await;
    }

    Ok((observed_positive_lag, observed_lsn_divergence, last_lag))
}

#[tokio::test]
async fn test_replica_collector_reports_positive_lag_on_paused_replay() -> Result<()> {
    let (_guard, primary_pool, replica_pool) = start_replication_topology().await?;
    initialize_replication_lag_test_table(&primary_pool).await?;
    wait_until_baseline_replayed(&replica_pool).await?;
    assert_initial_replica_snapshot(&replica_pool).await?;
    pause_replay(&replica_pool).await?;
    generate_wal_burst(&primary_pool, 2_000).await?;
    let (observed_positive_lag, observed_lsn_divergence, last_lag) =
        observe_positive_lag_with_parity(&primary_pool, &replica_pool).await?;

    let _ = sqlx::query("SELECT pg_wal_replay_resume()")
        .execute(&replica_pool)
        .await;

    primary_pool.close().await;
    replica_pool.close().await;

    assert!(
        observed_positive_lag,
        "expected pg_replication_lag_seconds > 0 while replay is paused; lsn_diverged={observed_lsn_divergence}, last_lag={last_lag}"
    );

    Ok(())
}
