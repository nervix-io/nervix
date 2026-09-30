//! Layer: data plane.
//! Owns: one synchronization round of the branch states a replica keeps for a branch-keyed entity,
//! and the owner those rounds ask for checkpoints.
//! May depend on: the branch lifecycle a replica holds, replica installation, the committed state
//! identities and the interconnect dispatcher.
//! Must not know: how an owner answers a request, what a checkpoint holds, NSPL parsing, or
//! control-plane transactions.

use super::*;

/// The owner of the runtime state a replica keeps, as the replica asks it for checkpoints.
///
/// A replica task asks the owner over the interconnect. A benchmark reaches an owner runtime in the
/// same process through the same calls, so what it measures is the replica's side of the exchange.
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
}

/// The owner of a replicated state, asked over the interconnect with a deadline for each answer.
pub(in crate::runtime) struct RemoteStateOwner {
    runtime: Runtime,
    node: ClusterNodeName,
    response_timeout: Duration,
}

impl RemoteStateOwner {
    pub(in crate::runtime) fn new(
        runtime: Runtime,
        node: ClusterNodeName,
        response_timeout: Duration,
    ) -> Self {
        Self {
            runtime,
            node,
            response_timeout,
        }
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
                self.response_timeout,
            )
            .await
    }
}

impl Runtime {
    /// Synchronize, once, the branch lifecycle a replica holds for one branch-keyed entity from
    /// `owner`, and then every branch state of kind `state_kind` that lifecycle names.
    pub(super) async fn synchronize_replica_branch_states(
        &self,
        owner: &impl StateOwner,
        branch_lru: &RuntimeStatePlacement,
        lifecycle: &ReplicatedBranchLifecycle,
        state_kind: Option<RuntimeStateKind>,
    ) {
        let after_lsm = match self.passive_state_replica_lsm(branch_lru) {
            Ok(lsm) => lsm,
            Err(error) => {
                warn!(error = %error, "failed to read replicated branch lifecycle progress");
                None
            }
        };
        match owner.checkpoint_after(branch_lru, after_lsm).await {
            Ok(Some(snapshot)) => {
                if let Err(error) = self
                    .install_passive_state_replica_snapshot(owner.node(), branch_lru, snapshot)
                    .await
                {
                    warn!(error = %error, "failed to install replicated branch lifecycle checkpoint");
                    return;
                }
            }
            Ok(None) => {}
            Err(error) => {
                warn!(error = %error, "failed to sync replicated branch lifecycle state");
                return;
            }
        }
        let Some(state_kind) = state_kind else {
            return;
        };
        let Some(held) = lifecycle.latest() else {
            return;
        };
        let branches = match held.branches() {
            Ok(branches) => branches.keys(),
            Err(error) => {
                warn!(
                    error = %error,
                    "failed to decode replicated branch lifecycle checkpoint"
                );
                return;
            }
        };
        for branch in branches {
            nervix_primitives::task::consume_budget().await;
            let placement = match self.state_placement(
                &branch_lru.domain,
                state_kind,
                branch_lru.kind,
                branch_lru.identifier.clone(),
                branch,
            ) {
                Ok(placement) => placement,
                Err(error) => {
                    warn!(error = %error, "failed to place replicated branch state");
                    continue;
                }
            };
            let after_lsm = match self.passive_state_replica_lsm(&placement) {
                Ok(lsm) => lsm,
                Err(error) => {
                    warn!(error = %error, "failed to read replicated branch state progress");
                    continue;
                }
            };
            match owner.checkpoint_after(&placement, after_lsm).await {
                Ok(Some(snapshot)) => {
                    if let Err(error) = self
                        .install_passive_state_replica_snapshot(owner.node(), &placement, snapshot)
                        .await
                    {
                        warn!(error = %error, "failed to install replicated branch state checkpoint");
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    warn!(error = %error, "failed to sync replicated branch state");
                }
            }
        }
    }
}
