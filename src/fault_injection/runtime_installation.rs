//! Node-local runtime installation controls for public recovery scenarios.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Bounded installation refusals, installation, preparation and readiness pauses, and
//!   the forced-recovery observation that releases a held initial installation.
//! - **Depends on.** The fault harness's shared seam registry, typed node and domain identities and
//!   pause points, the committed domain status, and governed atomic ordering.
//! - **Must not know.** Graph construction, schedules, connector drivers, or persisted runtime state.

use std::collections::BTreeMap;

use nervix_models::{ClusterNodeName, DomainName, DomainState, DomainStatus};
use nervix_primitives::sync::atomic::Ordering;

use super::{CommandPausePoint, FaultInjection};

/// Whether this node still owes a bounded construction refusal, or delivered them all.
#[derive(Debug, Clone, Copy)]
pub(super) enum RuntimeInstallationRefusals {
    Remaining(std::num::NonZeroUsize),
    Delivered,
}

impl RuntimeInstallationRefusals {
    fn is_delivered(&self) -> bool {
        matches!(self, Self::Delivered)
    }

    fn take_refusal(&mut self) -> bool {
        let Self::Remaining(remaining) = *self else {
            return false;
        };
        *self = match std::num::NonZeroUsize::new(remaining.get() - 1) {
            Some(remaining) => Self::Remaining(remaining),
            None => Self::Delivered,
        };
        true
    }
}

impl FaultInjection {
    /// Holds the next runtime installation the node begins, inside its installation sequence and
    /// before it synchronizes any domain lifecycle or schedule of the admitted revision.
    pub fn pause_runtime_installation_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::RuntimeInstallation(node_id));
    }

    /// Refuses exactly `count` installations on this node after linearizable startup admission.
    pub fn refuse_runtime_installations_on(
        &self,
        node_id: ClusterNodeName,
        count: std::num::NonZeroUsize,
    ) {
        self.inner
            .refused_runtime_installations
            .insert(node_id, RuntimeInstallationRefusals::Remaining(count));
    }

    pub fn runtime_installation_refusals_delivered(&self, node_id: &ClusterNodeName) -> bool {
        self.inner
            .refused_runtime_installations
            .get(node_id)
            .is_some_and(|refusals| refusals.is_delivered())
    }

    pub(crate) fn take_runtime_installation_refusal(&self, node_id: &ClusterNodeName) -> bool {
        let Some(mut refusals) = self.inner.refused_runtime_installations.get_mut(node_id) else {
            return false;
        };
        refusals.take_refusal()
    }

    pub async fn wait_for_runtime_installation_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::RuntimeInstallation(node_id.clone()))
            .await;
    }

    pub fn release_runtime_installation_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::RuntimeInstallation(node_id.clone()));
    }

    /// Resume a held initial installation only after forced recovery observes its missing
    /// schedule. The observation precedes release, so the fixture never races two scenario steps
    /// against the recovery budget.
    pub fn resume_installation_after_missing_recovery_schedule(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::ForcedRecoveryMissingSchedule(node_id));
    }

    pub async fn wait_for_missing_recovery_schedule(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::ForcedRecoveryMissingSchedule(
            node_id.clone(),
        ))
        .await;
    }

    pub(crate) fn missing_recovery_schedule_if_armed(&self, node_id: &ClusterNodeName) {
        let point = CommandPausePoint::ForcedRecoveryMissingSchedule(node_id.clone());
        let pause = match self.inner.command_pauses.get(&point) {
            Some(pause) => pause.value().clone(),
            None => return,
        };
        if pause.claimed.swap(true, Ordering::AcqRel) {
            return;
        }
        pause.reach();
        self.release_runtime_installation_pause(node_id);
        // Keep the one-shot observation until the scenario observes it or finishes cleanup.
    }

    pub fn pause_runtime_preparation_on(&self, node_id: ClusterNodeName) {
        self.arm_command_pause(CommandPausePoint::RuntimePreparation(node_id));
    }

    pub async fn wait_for_runtime_preparation_pause(&self, node_id: &ClusterNodeName) {
        self.wait_for_command_pause(&CommandPausePoint::RuntimePreparation(node_id.clone()))
            .await;
    }

    pub fn release_runtime_preparation_pause(&self, node_id: &ClusterNodeName) {
        self.release_command_pause(&CommandPausePoint::RuntimePreparation(node_id.clone()));
    }

    /// Holds this node's readiness for the first runtime revision it applies that runs `domain`,
    /// as a node slow to become ready would: the node has prepared the revision and started its
    /// ingestors, and reports the revision ready only once the scenario releases it.
    pub fn pause_runtime_readiness_on(&self, node_id: ClusterNodeName, domain: DomainName) {
        self.arm_command_pause(CommandPausePoint::RuntimeReadiness {
            node: node_id,
            domain,
        });
    }

    pub async fn wait_for_runtime_readiness_pause(
        &self,
        node_id: &ClusterNodeName,
        domain: &DomainName,
    ) {
        self.wait_for_command_pause(&CommandPausePoint::RuntimeReadiness {
            node: node_id.clone(),
            domain: domain.clone(),
        })
        .await;
    }

    pub fn release_runtime_readiness_pause(&self, node_id: &ClusterNodeName, domain: &DomainName) {
        self.release_command_pause(&CommandPausePoint::RuntimeReadiness {
            node: node_id.clone(),
            domain: domain.clone(),
        });
    }

    /// Holds the readiness of a revision this node applies while a scenario holds it for one of
    /// the revision's running domains.
    pub(crate) async fn pause_runtime_readiness_if_armed(
        &self,
        node_id: &ClusterNodeName,
        domains: &BTreeMap<DomainName, DomainState>,
    ) {
        for (domain, state) in domains {
            nervix_primitives::task::consume_budget().await;
            if let DomainStatus::Running = state.status {
                self.pause_command_if_armed(CommandPausePoint::RuntimeReadiness {
                    node: node_id.clone(),
                    domain: domain.clone(),
                })
                .await;
            }
        }
    }
}
