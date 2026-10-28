//! Public snapshot receive scenarios using identities from the archive's validated descriptors.
//! Layer: test harness.
//! Owns: bounded long branch keys and complete archived placement selection.
//! Depends on: public archives and the authenticated snapshot probe.
//! Must not know: runtime placement maps or native checkpoint encoding.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_backup::DescribedRuntimeState;
use nervix_interconnect::{RuntimeState, StatePlacementEnvelope};
use nervix_models::ModelKind;

use super::*;

#[given(expr = "two snapshot tenant placeholders contain {int} bytes each")]
fn snapshot_tenant_placeholders(world: &mut ScenarioWorld, bytes: usize) {
    assert!((1000..=3000).contains(&bytes));
    world
        .placeholders
        .insert("snapshot_alpha".to_owned(), "a".repeat(bytes));
    world
        .placeholders
        .insert("snapshot_beta".to_owned(), "b".repeat(bytes));
}

#[then(
    expr = "snapshot checkpoints in backup {string} refuse overlapping receive memory on node \
            {string} and transfer completely after release"
)]
async fn snapshot_receive_admission(world: &mut ScenarioWorld, file: String, node: String) {
    let archive = described_archive(archive_path(world, &file)).await;
    let domain = scenario_domain(world, &world.domain);
    let entry = archive
        .domains
        .iter()
        .find(|entry| entry.record.domain == domain)
        .assured("the archive contains the populated domain");
    let mut placements = Vec::new();
    for state in &entry.state {
        if let DescribedRuntimeState::Deduplicator { descriptor, .. } = state {
            assert_eq!(
                descriptor.keys, 2,
                "both interleaved rows survive in each branch"
            );
            let branch = descriptor
                .branch
                .clone()
                .assured("the fixture state is branched");
            placements.push(StatePlacementEnvelope {
                domain: descriptor.domain.clone(),
                state: RuntimeState::Deduplicator {
                    schema: descriptor.schema,
                },
                kind: ModelKind::Deduplicator,
                identifier: descriptor.entity.clone(),
                branch_key: Some(
                    branch
                        .into_iter()
                        .map(nervix_backup::StateField::into_remote)
                        .collect(),
                ),
            });
        }
    }
    assert_eq!(
        placements.len(),
        2,
        "both tenant checkpoint identities are archived"
    );
    world
        .cluster()
        .verify_snapshot_receive_admission(&node, placements)
        .await
        .assured("both actual snapshot receivers refuse promptly and recover complete checkpoints");
}
