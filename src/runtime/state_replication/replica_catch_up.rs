//! Layer: data plane.
//! Owns: one catch-up round of the replica task that keeps a branch-keyed entity current, and the
//! owner those rounds ask for the entity's lifecycle, catalog and checkpoints.
//! May depend on: the branch lifecycle a replica keeps, the replica's record of the entity's branch
//! checkpoints, replica installation and the interconnect dispatcher.
//! Must not know: how an owner answers a request or keeps its catalog, what a checkpoint holds,
//! NSPL parsing, or control-plane transactions.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "a replica task's catch-up round runs every replication poll interval and on \
                  every announcement that wakes the task"
    )
)]

use nervix_interconnect::{
    BranchCheckpointCursor, BranchCheckpointListingRequest, StateSyncRequest,
};

use super::*;
use crate::runtime::branch_lifecycle_state::NamedBranches;

/// How many branch checkpoints one round of a replica task fetches and installs at a time.
const REPLICA_CATCH_UP_CONCURRENCY: usize = 16;

/// How many catalog pages one round reads at most. A round that stops short resumes from the
/// cursor it reached in the next round.
const REPLICA_CATCH_UP_PAGES_PER_ROUND: usize = 64;

/// The owner of the runtime state a replica keeps, as the replica task asks it for checkpoints.
///
/// A replica task asks the owner over the interconnect. A benchmark reaches an owner runtime in the
/// same process through the same calls, so what it measures is the replica's side of the exchange.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "a replica task asks its owner for the lifecycle, the catalog's pages and the \
                  checkpoints of every round"
    )
)]
pub(in crate::runtime) trait StateOwner: Send + Sync {
    /// The node that owns the state.
    fn node(&self) -> &ClusterNodeName;

    /// The owner's checkpoint of `placement` when it holds one newer than `after_lsm`.
    fn checkpoint_after(
        &self,
        placement: &RuntimeStatePlacement,
        after_lsm: Option<u64>,
    ) -> impl Future<
        Output = error_stack::Result<Option<PersistedRuntimeStateEntry>, StateReplicationError>,
    > + Send;

    /// The first changes after `after` in the owner's catalog of the branch checkpoints of the
    /// entity whose branch lifecycle `lifecycle` places.
    fn checkpoint_listing(
        &self,
        lifecycle: &RuntimeStatePlacement,
        after: Option<BranchCheckpointCursor>,
    ) -> impl Future<Output = error_stack::Result<OwnerCheckpointListing, StateReplicationError>> + Send;
}

/// The owner of a replicated state, asked over the interconnect with a deadline for each answer.
pub(in crate::runtime) struct RemoteStateOwner {
    runtime: Runtime,
    node: ClusterNodeName,
}

impl RemoteStateOwner {
    pub(in crate::runtime) fn new(runtime: Runtime, node: ClusterNodeName) -> Self {
        Self { runtime, node }
    }
}

impl StateOwner for RemoteStateOwner {
    fn node(&self) -> &ClusterNodeName {
        &self.node
    }

    async fn checkpoint_after(
        &self,
        placement: &RuntimeStatePlacement,
        after_lsm: Option<u64>,
    ) -> error_stack::Result<Option<PersistedRuntimeStateEntry>, StateReplicationError> {
        self.runtime
            .request_state_sync_with_timeout(
                &self.node,
                placement,
                after_lsm,
                StateSyncRequest::TIMEOUT,
            )
            .await
    }

    async fn checkpoint_listing(
        &self,
        lifecycle: &RuntimeStatePlacement,
        after: Option<BranchCheckpointCursor>,
    ) -> error_stack::Result<OwnerCheckpointListing, StateReplicationError> {
        let request_failed = || StateReplicationError::Request {
            target: self.node.clone(),
            placement: lifecycle.clone(),
        };
        let Some(dispatcher) = self.runtime.inner.remote_dispatcher.load_full() else {
            return Err(Report::new(StateReplicationError::DispatcherUnavailable {
                target: self.node.clone(),
                placement: lifecycle.clone(),
            }));
        };
        let response = dispatcher
            .request_with_timeout(
                &self.node,
                BranchCheckpointListingRequest {
                    lifecycle: lifecycle.to_remote(),
                    after,
                },
                BranchCheckpointListingRequest::TIMEOUT,
            )
            .await
            .change_context_lazy(request_failed)?;
        let listing = response.result.map_err(|failure| {
            Report::new(StateReplicationError::RemoteFailure {
                target: self.node.clone(),
                placement: lifecycle.clone(),
                failure,
            })
        })?;
        OwnerCheckpointListing::from_remote(listing).change_context_lazy(|| {
            StateReplicationError::UndecodableListing {
                target: self.node.clone(),
                placement: lifecycle.clone(),
            }
        })
    }
}

/// One planned step of a round, and what happened to it.
struct SettledStep {
    step: BranchStep,
    outcome: StepOutcome,
}

impl Runtime {
    /// One round in which the replica task of a branch-keyed entity catches up with `owner`.
    ///
    /// The task synchronizes the entity's branch lifecycle, which it keeps in `lifecycle`, and,
    /// when the entity keeps state per branch, reads what changed in the owner's catalog of the
    /// entity's branch checkpoints since its previous round. Only the branches that changed, that
    /// the lifecycle names for the first time, that the owner announced, or whose earlier step
    /// failed are looked at, so a round in which no branch changed sends two requests however
    /// many branches the entity has, and reaches no shared map for any branch.
    pub(super) async fn catch_up_replica_branches(
        &self,
        owner: &impl StateOwner,
        branch_lru: &RuntimeStatePlacement,
        lifecycle: &ReplicatedBranchLifecycle,
        checkpoints: &mut ReplicaBranchCheckpoints,
        branch_states: Option<RuntimeStateKind>,
    ) {
        let announced = lifecycle.take_announcements();
        self.catch_up_replica_lifecycle(
            owner,
            branch_lru,
            lifecycle,
            checkpoints,
            announced.lifecycle,
        )
        .await;
        if branch_states.is_none() {
            return;
        }
        self.read_owner_checkpoint_catalog(owner, branch_lru, checkpoints)
            .await;
        checkpoints.take_announced(announced.branches);
        // Before this node holds a lifecycle, it names no branch: only unbranched work is caught
        // up then.
        let unnamed = NamedBranches::default();
        let held = lifecycle.latest();
        let named = match held.as_ref() {
            Some(held) => match held.branches() {
                Ok(named) => named,
                Err(error) => {
                    warn!(
                        error = %format_args!("{error:#}"),
                        "failed to decode replicated branch lifecycle checkpoint"
                    );
                    return;
                }
            },
            None => &unnamed,
        };
        let mut steps = checkpoints.plan(named).into_iter();
        let mut running = FuturesUnordered::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            while running.len() < REPLICA_CATCH_UP_CONCURRENCY {
                let Some(step) = steps.next() else {
                    break;
                };
                running.push(async move {
                    let outcome = self
                        .run_replica_branch_step(owner, branch_lru, lifecycle, &step)
                        .await;
                    SettledStep { step, outcome }
                });
            }
            let Some(settled) = running.next().await else {
                return;
            };
            checkpoints.settle(settled.step, settled.outcome);
        }
    }

    /// Synchronize the entity's branch lifecycle this replica keeps in `lifecycle` from `owner`,
    /// and acknowledge `announced`, the newest lifecycle revision the owner announced, when this
    /// replica already holds it.
    async fn catch_up_replica_lifecycle(
        &self,
        owner: &impl StateOwner,
        branch_lru: &RuntimeStatePlacement,
        lifecycle: &ReplicatedBranchLifecycle,
        checkpoints: &mut ReplicaBranchCheckpoints,
        announced: Option<u64>,
    ) {
        let previous = lifecycle.latest();
        let after_lsm = previous.as_ref().map(|previous| previous.lsm());
        let fetched = match owner.checkpoint_after(branch_lru, after_lsm).await {
            Ok(fetched) => fetched,
            Err(error) => {
                warn!(error = %format_args!("{error:#}"), "failed to sync replicated branch lifecycle state");
                return;
            }
        };
        let Some(snapshot) = fetched else {
            let Some(announced) = announced else {
                return;
            };
            let Some(held) = previous else {
                return;
            };
            if held.lsm() < announced {
                return;
            }
            // The owner waits for this node to acknowledge the revision it announced, and this
            // node's earlier acknowledgement of it may have been lost.
            if let Err(error) = self
                .acknowledge_durable_state_replica(owner.node(), branch_lru, held.lsm())
                .await
            {
                warn!(error = %format_args!("{error:#}"), "failed to acknowledge replicated branch lifecycle checkpoint");
            }
            return;
        };
        if let Err(error) = self
            .install_replica_branch_lifecycle(owner.node(), branch_lru, snapshot, lifecycle)
            .await
        {
            warn!(error = %format_args!("{error:#}"), "failed to install replicated branch lifecycle checkpoint");
            return;
        }
        let Some(current) = lifecycle.latest() else {
            return;
        };
        let current = match current.branches() {
            Ok(current) => current,
            Err(error) => {
                warn!(
                    error = %format_args!("{error:#}"),
                    "failed to decode replicated branch lifecycle checkpoint"
                );
                return;
            }
        };
        // A lifecycle read from storage is held without being decoded, and one that does not
        // decode named no branch the task acted on, so every branch the new one names is new to it.
        let previous = previous
            .as_ref()
            .and_then(|previous| previous.branches().ok());
        checkpoints.follow_lifecycle(previous, current);
    }

    /// Read the changes the owner's catalog of the entity's branch checkpoints lists after the
    /// cursor the replica task reached, page by page, up to a bounded number of pages.
    async fn read_owner_checkpoint_catalog(
        &self,
        owner: &impl StateOwner,
        branch_lru: &RuntimeStatePlacement,
        checkpoints: &mut ReplicaBranchCheckpoints,
    ) {
        for _ in 0..REPLICA_CATCH_UP_PAGES_PER_ROUND {
            nervix_primitives::task::consume_budget().await;
            let listing = match owner
                .checkpoint_listing(branch_lru, checkpoints.cursor())
                .await
            {
                Ok(listing) => listing,
                Err(error) => {
                    warn!(error = %format_args!("{error:#}"), "failed to read the owner's branch checkpoint catalog");
                    return;
                }
            };
            let OwnerCheckpointListing::Listed(listing) = listing else {
                checkpoints.forget_owner();
                return;
            };
            if !checkpoints.apply(listing) {
                return;
            }
        }
    }

    /// Carry out `step` for one branch: read what this node holds when the step does not know it
    /// yet, acknowledge an announced revision this node already holds, and otherwise fetch the
    /// owner's newer checkpoint and install it.
    async fn run_replica_branch_step(
        &self,
        owner: &impl StateOwner,
        branch_lru: &RuntimeStatePlacement,
        lifecycle: &ReplicatedBranchLifecycle,
        step: &BranchStep,
    ) -> StepOutcome {
        let placement = RuntimeStatePlacement {
            domain: branch_lru.domain.clone(),
            state: step.state,
            kind: branch_lru.kind,
            identifier: branch_lru.identifier.clone(),
            branch_key: step.branch.clone(),
        };
        if !lifecycle.placement_is_current(&placement) {
            return StepOutcome::Stale;
        }
        let held = match step.held {
            Some(held) => held,
            None => {
                let held = nervix_primitives::expect_lint!(
                    nervix::lifecycle_call,
                    "a replica task reads what this node holds of a branch once, when it first \
                     looks at the branch, and keeps that record itself from then on",
                    self.held_branch_checkpoint(&placement, lifecycle)
                );
                match held {
                    Ok(held) => held,
                    Err(error) => {
                        warn!(error = %format_args!("{error:#}"), "failed to read replicated branch state progress");
                        return StepOutcome::Failed(None);
                    }
                }
            }
        };
        if held.covers(step.target) {
            if step.acknowledge
                && let Held::Revision(lsm) = held
                && let Err(error) = self
                    .acknowledge_durable_state_replica(owner.node(), &placement, lsm)
                    .await
            {
                warn!(error = %format_args!("{error:#}"), "failed to acknowledge replicated branch state checkpoint");
                return StepOutcome::Failed(Some(held));
            }
            return StepOutcome::Settled(held);
        }
        let snapshot = match owner.checkpoint_after(&placement, held.after_lsm()).await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => return StepOutcome::Settled(held),
            Err(error) => {
                warn!(error = %format_args!("{error:#}"), "failed to sync replicated branch state");
                return StepOutcome::Failed(Some(held));
            }
        };
        match self
            .install_replica_branch_checkpoint(owner.node(), &placement, snapshot, lifecycle, held)
            .await
        {
            Ok(installed) => StepOutcome::Settled(installed),
            Err(error) => {
                warn!(error = %format_args!("{error:#}"), "failed to install replicated branch state checkpoint");
                StepOutcome::Failed(Some(held))
            }
        }
    }
}

#[cfg(test)]
#[path = "replica_catch_up_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "remote_owner_tests.rs"]
mod remote_owner_tests;
