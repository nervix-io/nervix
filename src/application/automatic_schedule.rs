//! Publication of one automatic scheduling decision.
//!
//! Layer: control plane.
//! - **Owns.** Preparing forced moves, revalidating their inputs and publishing or rejecting them.
//! - **Depends on.** Cluster availability, committed planning inputs and the recovery coordinator.
//! - **Must not know.** Placement rules, checkpoint contents or how a destination installs them.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns automatic schedule publication")
)]

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{AutomaticScheduleInput, ConsensusError, GossipState};
use nervix_interconnect::ForcedOwnershipRecoveryRound;
use nervix_models::{ClusterNodeIncarnation, ClusterNodeName, DomainSchedule};
use tracing::{info, warn};

use super::{
    ownership_handoff::{ForcedOwnershipRecoveryCoordinator, ForcedRecoveryPreparations},
    schedule_planning::DomainSchedulePlanningSnapshot,
    session_service::SessionServiceImpl,
};

pub(super) struct AutomaticSchedulePublication<'a> {
    pub(super) committed: &'a AutomaticScheduleInput,
    pub(super) availability: &'a GossipState,
    pub(super) health_revision: u64,
    pub(super) live_node_incarnations: &'a BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
}

impl SessionServiceImpl {
    pub(super) async fn publish_automatic_schedule(
        &self,
        inputs: &AutomaticSchedulePublication<'_>,
        current: Option<&DomainSchedule>,
        mut schedule: DomainSchedule,
        trigger: &'static str,
    ) -> bool {
        let domain = schedule.domain.clone();
        let fence = inputs.committed.fence();
        let mut preparations = None;
        if let Some(current) = current {
            let coordination = self
                .inner
                .interconnect
                .next_coordination_identity()
                .assured("one running process cannot exhaust its coordination sequence");
            let round = ForcedOwnershipRecoveryRound {
                leader_term: fence.leader_tenure().term(),
                coordination,
            };
            let attempts = preparations.insert(ForcedRecoveryPreparations::new(self, round));
            ForcedOwnershipRecoveryCoordinator {
                recovery_budget: inputs
                    .committed
                    .runtime_state()
                    .parameters
                    .verified("scheduling starts after committed policy initialization")
                    .values
                    .forced_recovery_budget(),
                runtime: &self.inner.runtime,
                interconnect: &self.inner.interconnect,
                local_node_id: self.inner.consensus.local_node_id(),
                node_incarnations: inputs.live_node_incarnations,
            }
            .prepare_schedule(current, &mut schedule, attempts)
            .await;
            #[cfg(feature = "testing")]
            self.inner
                .runtime
                .fault_injection()
                .observe_forced_recovery_preparation(&domain, attempts.confirmed)
                .await;
        }

        let unpublished_reason = if !self
            .inner
            .cluster
            .peer_health_scheduling_revision_is_current(inputs.health_revision)
        {
            Some("the peer-health inputs to availability changed")
        } else {
            let current_availability = self.inner.cluster.availability_state().await;
            let topology = inputs.committed.topology();
            (!DomainSchedulePlanningSnapshot::same_eligibility(
                inputs.availability,
                &current_availability,
                topology.voters(),
                topology.cordoned(),
            ))
            .then_some("live or placement-eligible voter incarnations changed")
        };
        if let Some(reason) = unpublished_reason {
            info!(
                domain = domain.as_str(),
                trigger, reason, "discarded an automatic schedule before publication"
            );
            return false;
        }

        let planning = inputs.committed.planning_inputs(&domain).cloned().verified(
            "the automatic runtime snapshot and planning inputs contain the same domains",
        );
        if let Some(attempts) = preparations.as_mut() {
            attempts.begin_publication();
        }
        match self
            .inner
            .consensus
            .apply_automatic_domain_schedule(fence, planning, Some(schedule))
            .await
        {
            Ok(()) => true,
            Err(error) => {
                if matches!(
                    error.current_context(),
                    ConsensusError::Conflict(_) | ConsensusError::LeadershipLost { .. }
                ) && let Some(attempts) = preparations.as_mut()
                {
                    attempts.reject_publication();
                }
                warn!(domain = domain.as_str(), trigger, error = %format_args!("{error:#}"), "failed to publish an automatic schedule");
                false
            }
        }
    }
}
