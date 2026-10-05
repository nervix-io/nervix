//! Public observations of restore checkpoint staging.
//!
//! Layer: test harness.
//! - **Owns.** Waiting for node metrics to prove reclamation and bounded usage.
//! - **Depends on.** The public metrics endpoint and scenario cluster.
//! - **Must not know.** Database keys or the cleanup implementation.

use super::*;

#[given("the cluster's reclaimed restore bytes are saved")]
async fn given_reclaimed_restore_bytes(world: &mut ScenarioWorld) {
    let bytes = reclaimed_restore_bytes(world).await;
    world
        .placeholders
        .insert("reclaimed_restore_bytes".to_string(), bytes.to_string());
}

#[then(expr = "at least {int} MiB of replaced restore chunks are reclaimed")]
async fn then_restore_chunks_reclaimed(world: &mut ScenarioWorld, mebibytes: u64) {
    let baseline: f64 = world.placeholders["reclaimed_restore_bytes"]
        .parse()
        .assured("the preceding measurement saved a numeric byte count");
    let expected: f64 = mebibytes
        .checked_mul(1024 * 1024)
        .assured("the scenario's MiB count fits u64")
        .approx_into();
    let deadline = nervix_primitives::time::Instant::now() + Duration::from_secs(60);
    loop {
        nervix_primitives::task::consume_budget().await;
        let reclaimed = reclaimed_restore_bytes(world).await - baseline;
        if reclaimed >= expected {
            return;
        }
        assert!(
            nervix_primitives::time::Instant::now() < deadline,
            "maintenance reclaimed {reclaimed} bytes; expected at least {expected} after header \
             replacement or purge"
        );
        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn reclaimed_restore_bytes(world: &ScenarioWorld) -> f64 {
    let mut bytes = 0.0;
    for node in world.cluster().node_ids() {
        nervix_primitives::task::consume_budget().await;
        bytes += world
            .cluster()
            .read_observability_metric(&node, "nervix_restore_staging_reclaimed_bytes_total", &[])
            .await
            .assured("each node reports completed checkpoint reclamation");
    }
    bytes
}

#[given(expr = "restore checkpoint staging is limited to {int} bytes")]
fn given_restore_staging_limit(world: &mut ScenarioWorld, bytes: u64) {
    world.cluster_config.restore_staging_max_bytes = bytes;
}

#[then("restore checkpoint staging usage is zero on every node")]
async fn then_restore_staging_is_empty(world: &mut ScenarioWorld) {
    wait_for_restore_sweeps(world).await;
    for node in world.cluster().node_ids() {
        nervix_primitives::task::consume_budget().await;
        world
            .cluster()
            .wait_for_observability_metric_value(
                &node,
                "nervix_restore_staging_bytes",
                &[],
                0,
                Some(Duration::from_secs(60)),
            )
            .await
            .assured("every node releases staging after the restore ends");
    }
}

#[then("an active restore retains its staging through completed maintenance sweeps on every node")]
async fn then_active_restore_is_retained(world: &mut ScenarioWorld) {
    wait_for_restore_sweeps(world).await;
    let mut staged = 0.0;
    for node in world.cluster().node_ids() {
        nervix_primitives::task::consume_budget().await;
        let bytes = world
            .cluster()
            .read_observability_metric(&node, "nervix_restore_staging_bytes", &[])
            .await
            .assured("the retained staging is observable");
        let limit = world
            .cluster()
            .read_observability_metric(&node, "nervix_restore_staging_limit_bytes", &[])
            .await
            .assured("the node quota is observable");
        assert!(
            bytes <= limit,
            "active staging remains within its node's quota"
        );
        staged += bytes;
    }
    assert!(
        staged > 0.0,
        "active unpublished checkpoint data survived completed sweeps"
    );
}

#[then("abandoned restore storage is reclaimed across every node")]
async fn then_abandoned_restore_storage_is_reclaimed(world: &mut ScenarioWorld) {
    wait_for_restore_sweeps(world).await;
    let mut reclaimed = 0.0;
    for node in world.cluster().node_ids() {
        nervix_primitives::task::consume_budget().await;
        world
            .cluster()
            .wait_for_observability_metric_value(
                &node,
                "nervix_restore_staging_bytes",
                &[],
                0,
                Some(Duration::from_secs(60)),
            )
            .await
            .assured("every node reclaims its unpublished checkpoint keys");
        reclaimed += world
            .cluster()
            .read_observability_metric(&node, "nervix_restore_staging_reclaimed_bytes_total", &[])
            .await
            .assured("every node reports its reclaimed storage");
        let limit = world
            .cluster()
            .read_observability_metric(&node, "nervix_restore_staging_limit_bytes", &[])
            .await
            .assured("the quota is publicly observable");
        assert!(
            limit > 0.0,
            "the node enforces a finite positive staging quota"
        );
    }
    assert!(
        reclaimed > 0.0,
        "the failed restore left positively observed reclaimed bytes"
    );
}

async fn wait_for_restore_sweeps(world: &ScenarioWorld) {
    let mut previous = Vec::new();
    for node in world.cluster().node_ids() {
        nervix_primitives::task::consume_budget().await;
        let sweeps: i64 = world
            .cluster()
            .read_observability_metric(&node, "nervix_restore_staging_sweeps_total", &[])
            .await
            .assured("the maintenance counter is observable")
            .checked_approx_into()
            .assured("short scenario sweep counts fit i64");
        previous.push((node, sweeps));
    }
    for (node, sweeps) in previous {
        nervix_primitives::task::consume_budget().await;
        world
            .cluster()
            .wait_for_observability_metric_at_least(
                &node,
                "nervix_restore_staging_sweeps_total",
                &[],
                sweeps + 2,
                Some(Duration::from_secs(60)),
            )
            .await
            .assured("every node completed two fresh maintenance sweeps");
    }
}
