//! Controlled boundaries of the window between a planned handoff's confirmation and its schedule
//! commit, for public handoff scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The domain-scoped pause a coordinator takes after every participant confirmed the
//!   prepared moves of a planned handoff and before it commits the handoff's schedule, and the
//!   count of ownership-handoff preparation reconciliations each observed node completes.
//! - **Depends on.** The shared one-shot pause owner, the boundary's watch channel and vocabulary
//!   identities.
//! - **Must not know.** Plexus syntax, handoff planning or reconciliation policy.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{ClusterNodeName, DomainName};
use nervix_primitives::sync::watch;

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    /// Holds the coordinator of the next planned handoff in `domain`, once, after every
    /// participant confirmed the handoff's prepared moves and before it commits the handoff's
    /// schedule.
    pub fn pause_ownership_handoff_commit(&self, domain: DomainName) {
        self.arm_command_pause(CommandPausePoint::OwnershipHandoffCommit(domain));
    }

    pub async fn wait_for_ownership_handoff_commit_pause(&self, domain: &DomainName) {
        self.wait_for_command_pause(&CommandPausePoint::OwnershipHandoffCommit(domain.clone()))
            .await;
    }

    pub fn release_ownership_handoff_commit_pause(&self, domain: &DomainName) {
        self.release_command_pause(&CommandPausePoint::OwnershipHandoffCommit(domain.clone()));
    }

    pub(crate) async fn pause_ownership_handoff_commit_if_armed(&self, domain: &DomainName) {
        self.pause_command_if_armed(CommandPausePoint::OwnershipHandoffCommit(domain.clone()))
            .await;
    }

    /// Counts every ownership-handoff preparation reconciliation `node` completes from now on,
    /// starting again from zero.
    pub fn observe_ownership_handoff_reconciliations(&self, node: ClusterNodeName) {
        let (observed, _) = watch::channel(ObservedOwnershipHandoffReconciliations::default());
        self.inner
            .ownership_handoff_reconciliations
            .insert(node, observed);
    }

    /// Records that `node` completed one ownership-handoff preparation reconciliation, which
    /// durably discarded `discarded` preparations, when a scenario observes the node.
    pub(crate) fn record_ownership_handoff_reconciliation(
        &self,
        node: &ClusterNodeName,
        discarded: u64,
    ) {
        let Some(observed) = self.inner.ownership_handoff_reconciliations.get(node) else {
            return;
        };
        observed.send_modify(|observed| {
            observed.completed = observed
                .completed
                .checked_add(1)
                .assured("a scenario observes far fewer than u64::MAX reconciliations");
            observed.discarded = observed
                .discarded
                .checked_add(discarded)
                .assured("a scenario's preparations number far fewer than u64::MAX");
        });
    }

    /// Waits until `node` completed an ownership-handoff preparation reconciliation since the
    /// scenario began observing it, and returns what its reconciliations did so far.
    pub async fn wait_for_ownership_handoff_reconciliation(
        &self,
        node: &ClusterNodeName,
    ) -> ObservedOwnershipHandoffReconciliations {
        let mut observed = {
            let reconciliations = self
                .inner
                .ownership_handoff_reconciliations
                .get(node)
                .assured("a scenario waits only for the reconciliations of a node it observes");
            reconciliations.subscribe()
        };
        let reconciled = observed
            .wait_for(|observed| observed.completed > 0)
            .await
            .assured("an observed node keeps its sender until the scenario ends");
        *reconciled
    }
}

/// What an observed node's ownership-handoff preparation reconciliations did since a scenario
/// began observing it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObservedOwnershipHandoffReconciliations {
    /// The reconciliations the node completed.
    pub completed: u64,
    /// The preparations those reconciliations durably discarded.
    pub discarded: u64,
}
