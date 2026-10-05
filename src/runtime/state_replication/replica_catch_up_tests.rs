//! Layer: test harness.
//! Owns: owner regressions for the round in which a replica task catches the branch states of an
//! entity up: the requests a round sends its owner, the branches it fetches, and the branches it
//! looks at again.
//! May depend on: runtime internals and an owner runtime the replica reaches in the same process.
//! Must not know: the interconnect, production control-plane orchestration or edge protocols.

use std::time::Duration;

use nervix_models::{DomainNodeRef, ModelKind, ModelName, SchemaFingerprint, Timestamp};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    blocking::Mutex,
};

use super::*;

const DEDUPLICATOR: &str = "dedup_orders";

/// The revision of the branch lifecycle the owner and its caught-up replica hold.
const LIFECYCLE: u64 = 1;

/// A request a replica round sent its owner.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OwnerRequest {
    /// The entity's branch lifecycle, when newer than the revision the replica holds.
    Lifecycle { after_lsm: Option<u64> },
    /// One page of the owner's catalog of the entity's branch checkpoints.
    Listing,
    /// The checkpoint of one branch, when newer than the revision the replica holds.
    Checkpoint {
        branch: Option<BranchKey>,
        after_lsm: Option<u64>,
    },
}

/// An owner runtime a replica reaches in the same process, recording every request it answers.
struct InProcessOwner {
    runtime: Runtime,
    node: ClusterNodeName,
    requests: Mutex<Vec<OwnerRequest>>,
    /// Whether every catalog read fails, as one does when the owner does not answer in time.
    listing_fails: AtomicBool,
}

impl InProcessOwner {
    fn record(&self, request: OwnerRequest) {
        self.requests.lock().push(request);
    }
}

impl StateOwner for InProcessOwner {
    fn node(&self) -> &ClusterNodeName {
        &self.node
    }

    async fn checkpoint_after(
        &self,
        placement: &RuntimeStatePlacement,
        after_lsm: Option<u64>,
    ) -> error_stack::Result<Option<PersistedRuntimeStateEntry>, StateReplicationError> {
        let request = if placement.state.kind() == RuntimeStateKind::BranchLru {
            OwnerRequest::Lifecycle { after_lsm }
        } else {
            OwnerRequest::Checkpoint {
                branch: placement.branch_key.clone(),
                after_lsm,
            }
        };
        self.record(request);
        self.runtime
            .capture_state_checkpoint(placement, after_lsm)
            .await
    }

    async fn checkpoint_listing(
        &self,
        lifecycle: &RuntimeStatePlacement,
        after: Option<BranchCheckpointCursor>,
    ) -> error_stack::Result<OwnerCheckpointListing, StateReplicationError> {
        self.record(OwnerRequest::Listing);
        if self.listing_fails.load(Ordering::Relaxed) {
            return Err(Report::new(StateReplicationError::Request {
                target: self.node.clone(),
                placement: lifecycle.clone(),
            }));
        }
        Ok(self.runtime.branch_checkpoint_listing(lifecycle, after))
    }
}

/// The owner of a deduplicator whose branch lifecycle names a number of branches, and a replica
/// holding that lifecycle and a copy of every branch state the owner holds, as a replica does once
/// it caught up.
///
/// The replica holds no replica assignment, so it refuses every checkpoint it fetches: a unit test
/// observes what a round asks for, and the public replication scenarios what it installs.
struct CaughtUpReplica {
    owner: InProcessOwner,
    replica: Runtime,
    branch_lru: RuntimeStatePlacement,
    lifecycle: Arc<ReplicatedBranchLifecycle>,
    checkpoints: ReplicaBranchCheckpoints,
    branches: Vec<Option<BranchKey>>,
    states: Vec<Arc<ReplicatedDeduplicatorState>>,
}

impl CaughtUpReplica {
    fn new(branch_count: usize) -> Self {
        let domain = domain("default");
        let deduplicator = named::<ModelName>(DEDUPLICATOR);
        let owner = Runtime::default();
        let replica = Runtime::default();
        for runtime in [&owner, &replica] {
            publish_state_identity(
                runtime,
                &domain,
                ModelKind::Deduplicator,
                deduplicator.clone(),
            );
        }
        let branch_lru = owner
            .state_placement(
                &domain,
                RuntimeStateKind::BranchLru,
                ModelKind::Deduplicator,
                deduplicator.clone(),
                None,
            )
            .expect("the published identity places the branch lifecycle");
        let lifecycle = replica.replicated_branch_lifecycle(&branch_lru);
        let mut branches = Vec::new();
        let mut states = Vec::new();
        for index in 0..branch_count {
            let branch = string_branch_key("tenant", &format!("tenant-{index}"));
            let placement = owner
                .state_placement(
                    &domain,
                    RuntimeStateKind::Deduplicator,
                    ModelKind::Deduplicator,
                    deduplicator.clone(),
                    branch.clone(),
                )
                .expect("the published identity places the branch state");
            let state = owner
                .replicated_deduplicator_state(placement.clone())
                .expect("an empty deduplicator state initializes");
            let checkpoint = state
                .latest_snapshot()
                .expect("an empty deduplicator state encodes");
            lifecycle.hold_passive_checkpoint(&placement, checkpoint);
            branches.push(branch);
            states.push(state);
        }
        let named_branches = branch_lifecycle_snapshot(LIFECYCLE, &branches);
        owner
            .replicated_branch_lifecycle(&branch_lru)
            .publish(named_branches.clone());
        replica
            .install_replica_branch_lru_snapshot(&branch_lru, &lifecycle, named_branches)
            .expect("the branch lifecycle installs");
        Self {
            owner: InProcessOwner {
                runtime: owner,
                node: ClusterNodeName::parse("node-1").expect("valid node name"),
                requests: Mutex::new(Vec::new()),
                listing_fails: AtomicBool::new(false),
            },
            replica,
            branch_lru,
            lifecycle,
            checkpoints: ReplicaBranchCheckpoints::default(),
            branches,
            states,
        }
    }

    /// One round of the replica task, returning the requests it sent the owner in order.
    async fn round(&mut self) -> Vec<OwnerRequest> {
        self.replica
            .catch_up_replica_branches(
                &self.owner,
                &self.branch_lru,
                &self.lifecycle,
                &mut self.checkpoints,
                Some(RuntimeStateKind::Deduplicator),
            )
            .await;
        std::mem::take(&mut *self.owner.requests.lock())
    }

    /// Publish a new generation of the owner's state of branch `index`, without announcing it, and
    /// return its revision.
    fn revise(&self, index: usize) -> u64 {
        let state = &self.states[index];
        let mut keyspace = ReplicatedDeduplicatorState::keyspace(state);
        let reserved = keyspace.reserve_new_key(
            DeduplicatorKey::new(vec![ReorderKeyPart::Utf8(format!("txn-{index}"))]),
            Timestamp::from_unix_nanos(1),
            Duration::from_secs(600),
        );
        assert!(reserved, "a key the branch never saw is new");
        keyspace.publish();
        state.generations.load().revision
    }

    /// The state the owner keeps for every branch of the deduplicator.
    fn branch_state(&self) -> RuntimeState {
        RuntimeState::Deduplicator {
            schema: SchemaFingerprint::from_digest([7; 32]),
        }
    }

    /// The requests of a round that finds nothing to fetch.
    fn quiet_round(&self) -> Vec<OwnerRequest> {
        vec![
            OwnerRequest::Lifecycle {
                after_lsm: Some(LIFECYCLE),
            },
            OwnerRequest::Listing,
        ]
    }
}

#[nervix_primitives::test]
async fn a_round_in_which_no_branch_changed_asks_the_owner_twice_however_many_branches_it_has() {
    for branch_count in [16, 300] {
        let mut replica = CaughtUpReplica::new(branch_count);

        let pages = branch_count.div_ceil(BRANCH_CHECKPOINT_LISTING_PAGE.get());
        let mut first_round = vec![OwnerRequest::Lifecycle {
            after_lsm: Some(LIFECYCLE),
        }];
        first_round.extend(std::iter::repeat_n(OwnerRequest::Listing, pages));
        assert_eq!(
            replica.round().await,
            first_round,
            "the first round reads the whole catalog, and fetches no branch this node holds"
        );
        for _ in 0..3 {
            assert_eq!(replica.round().await, replica.quiet_round());
        }
    }
}

#[nervix_primitives::test]
async fn a_replica_fetches_only_the_branch_that_changed_and_again_until_it_holds_it() {
    let mut replica = CaughtUpReplica::new(16);
    replica.round().await;
    let revised = replica.revise(3);
    assert!(revised > 0, "the branch published a new generation");

    let mut fetches_revised_branch = replica.quiet_round();
    fetches_revised_branch.push(OwnerRequest::Checkpoint {
        branch: replica.branches[3].clone(),
        after_lsm: Some(0),
    });
    assert_eq!(
        replica.round().await,
        fetches_revised_branch,
        "the catalog lists the revision whose announcement was lost"
    );
    assert_eq!(
        replica.round().await,
        fetches_revised_branch,
        "a branch this node refused to install is fetched again, and no other"
    );
}

#[nervix_primitives::test]
async fn a_replica_drops_the_branches_of_a_state_its_schedule_no_longer_names() {
    let mut replica = CaughtUpReplica::new(4);
    replica.replica.publish_state_assignment(
        DomainNodeRef::node_in(
            domain("default"),
            ModelKind::Deduplicator,
            named::<ModelName>(DEDUPLICATOR),
        ),
        ScheduledStateAssignment {
            identity: ScheduledStateIdentity {
                schema_fingerprint: SchemaFingerprint::from_digest([8; 32]),
                wasm_state_generations: None,
            },
            checkpoint_owners: None,
        },
    );

    assert_eq!(replica.round().await, replica.quiet_round());
    replica.revise(2);
    assert_eq!(
        replica.round().await,
        replica.quiet_round(),
        "a revision of a state this node no longer places is never fetched"
    );
    let held = replica
        .lifecycle
        .latest()
        .expect("the replica holds the lifecycle");
    let named = held.branches().expect("the lifecycle decodes");
    assert!(
        replica.checkpoints.plan(named).is_empty(),
        "a dropped branch does not wait to be looked at again"
    );
}

#[nervix_primitives::test]
async fn an_announced_revision_the_replica_holds_is_not_fetched_again() {
    let mut replica = CaughtUpReplica::new(4);
    replica.round().await;

    replica.lifecycle.announce_lifecycle(LIFECYCLE);
    replica.lifecycle.announce_branch(
        replica.branches[2].clone(),
        AnnouncedCheckpoint {
            state: replica.branch_state(),
            lsm: 0,
        },
    );

    assert_eq!(replica.round().await, replica.quiet_round());
    assert_eq!(replica.round().await, replica.quiet_round());
}

#[nervix_primitives::test]
async fn an_announced_branch_is_fetched_while_the_catalog_cannot_be_read() {
    let mut replica = CaughtUpReplica::new(4);
    replica.round().await;
    let revised = replica.revise(1);
    replica.owner.listing_fails.store(true, Ordering::Relaxed);

    replica.lifecycle.announce_branch(
        replica.branches[1].clone(),
        AnnouncedCheckpoint {
            state: replica.branch_state(),
            lsm: revised,
        },
    );

    let mut fetches_announced_branch = replica.quiet_round();
    fetches_announced_branch.push(OwnerRequest::Checkpoint {
        branch: replica.branches[1].clone(),
        after_lsm: Some(0),
    });
    assert_eq!(replica.round().await, fetches_announced_branch);
    assert_eq!(
        replica.round().await,
        fetches_announced_branch,
        "an announcement this node could not acknowledge yet is kept for the next round"
    );
}
