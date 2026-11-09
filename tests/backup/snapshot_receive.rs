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

#[then(
    expr = "in a multi-node cluster backup {string} inferencer {string} is installed on its \
            scheduled owner and replicas"
)]
async fn restored_inferencer_replicas(world: &mut ScenarioWorld, file: String, inferencer: String) {
    if world.cluster().node_ids().len() == 1 {
        return;
    }
    let archive = described_archive(archive_path(world, &file)).await;
    let mut placements = Vec::new();
    for state in archive.domains.iter().flat_map(|domain| &domain.state) {
        if let DescribedRuntimeState::Inferencer { descriptor, .. } = state {
            if descriptor.entity.as_str() != inferencer {
                continue;
            }
            placements.push((
                StatePlacementEnvelope {
                    domain: scenario_domain(world, &world.domain),
                    state: RuntimeState::Inferencer {
                        schema: descriptor.schema,
                        model: descriptor.model,
                    },
                    kind: ModelKind::Inferencer,
                    identifier: descriptor.entity.clone(),
                    unit: descriptor.unit,
                    branch_key: descriptor.branch.as_ref().map(|fields| {
                        fields
                            .iter()
                            .cloned()
                            .map(nervix_backup::StateField::into_remote)
                            .collect()
                    }),
                },
                descriptor.revision,
            ));
        }
    }
    assert_eq!(
        placements.len(),
        2,
        "the source archive names both inference tensor frames"
    );
    let leader = current_leader_node(world).await;
    let status = world
        .cluster()
        .run_command(&leader, &world.domain, "SHOW CLUSTER STATUS;")
        .await
        .assured("the restored schedule is publicly visible");
    let (owner, replicas) =
        scheduled_node_placement_from_status(&status, &world.domain, "inferencer", &inferencer)
            .assured("the restored inference unit is scheduled");
    assert!(
        !replicas.is_empty(),
        "the three-node restore installs a replica"
    );
    assert_ne!(
        owner, leader,
        "the restore coordinator installs on a remote owner"
    );
    world
        .cluster()
        .verify_installed_inferencer_checkpoints(owner, &replicas, placements)
        .await
        .assured(
            "the installed owner and replicas contain both complete archived frames before START",
        );
}

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
                unit: descriptor.unit,
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

/// The archive supplies the production lifecycle identity without reconstructing a schema digest.
/// Tensor bytes themselves are then observed through authenticated checkpoint requests.
#[given(
    expr = "inferencer {string} has complete checkpoints for both branches on replica {string} of \
            owner {string}"
)]
async fn inferencer_replica_checkpoints(
    world: &mut ScenarioWorld,
    inferencer: String,
    replica: String,
    owner: String,
) {
    let replica = expand_placeholders(world, &replica);
    let owner = expand_placeholders(world, &owner);
    let file = archive_path(world, "inferencer-lifecycle.nvxb");
    let command = format!("BACKUP DOMAIN {} TO '{}';", world.domain, file.display());
    world
        .cluster()
        .run_command(&owner, &world.domain, &command)
        .await
        .expect("the public client captures lifecycle identities");
    let archive = described_archive(file).await;
    let domain = scenario_domain(world, &world.domain);
    let lifecycle = archive
        .domains
        .iter()
        .find(|d| d.record.domain == domain)
        .expect("the populated domain is archived")
        .state
        .iter()
        .find_map(|state| match state {
            DescribedRuntimeState::BranchLifecycle { lifecycle, .. }
                if lifecycle.owner_kind == ModelKind::Inferencer
                    && lifecycle.entity.as_str() == inferencer =>
            {
                Some(lifecycle.clone())
            }
            _ => None,
        })
        .expect("the inference branch lifecycle is archived");
    assert_eq!(
        lifecycle.branches.len(),
        2,
        "the fixture has both branch lifetimes"
    );
    let placement = StatePlacementEnvelope {
        domain,
        state: RuntimeState::BranchLru {
            schema: lifecycle.schema,
        },
        kind: ModelKind::Inferencer,
        identifier: lifecycle.entity,
        unit: lifecycle.unit,
        branch_key: None,
    };
    world
        .cluster()
        .wait_for_inferencer_replica_checkpoints(&owner, &replica, placement)
        .await
        .expect("both complete replica checkpoints cover the current branch publications");
}

#[then(expr = "within {string} inferencer {string} has no retained branch for tenant {string}")]
async fn inferencer_branch_evicted(
    world: &mut ScenarioWorld,
    within: String,
    inferencer: String,
    tenant: String,
) {
    let bound = parse_duration_text(&within).expect("a valid condition bound");
    let deadline = Instant::now() + bound;
    loop {
        let leader = current_leader_node(world).await;
        let file = archive_path(
            world,
            &format!(
                "inferencer-eviction-{}.nvxb",
                crate::common::raw_session::fresh_execution_reference()
            ),
        );
        let command = format!("BACKUP DOMAIN {} TO '{}';", world.domain, file.display());
        world
            .cluster()
            .run_command(&leader, &world.domain, &command)
            .await
            .expect("the public client captures branch lifecycle metadata");
        let archive = described_archive(file).await;
        let retained = archive
            .domains
            .iter()
            .flat_map(|domain| &domain.state)
            .filter_map(|state| match state {
                DescribedRuntimeState::BranchLifecycle { lifecycle, .. }
                    if lifecycle.owner_kind == ModelKind::Inferencer
                        && lifecycle.entity.as_str() == inferencer =>
                {
                    Some(lifecycle)
                }
                _ => None,
            })
            .flat_map(|lifecycle| &lifecycle.branches)
            .any(|branch| {
                branch.key.as_ref().is_some_and(|key| {
                    key.iter().any(|field| {
                        field.name == "tenant"
                            && field.value == nervix_backup::StateValue::String(tenant.clone())
                    })
                })
            });
        if !retained {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "inference branch {tenant:?} remains in the public lifecycle archive after {within}"
        );
        nervix_primitives::time::sleep(Duration::from_millis(25)).await;
    }
}
