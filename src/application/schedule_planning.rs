//! Immutable cluster inputs for synchronous domain-schedule planning.
//!
//! Layer: control plane.
//!
//! - **Owns.** Capturing one membership, liveness, eligibility and scheduler-policy snapshot.
//! - **Depends on.** Cluster observation, consensus topology and the registry scheduler decision.
//! - **Must not know.** Transaction syntax, persistence, runtime activation or commit progress.

use nervix_models::{ClusterNodeName, DomainName, DomainSchedule, PlacementPolicy};

use super::{
    ownership_handoff::{planned_relocation_count, prefer_former_owners_as_replicas},
    session_service::SessionServiceImpl,
};
use crate::registry::ActiveGraph;

/// A domain schedule computed from a candidate graph, with how many runtime nodes it moves off
/// the node that owns them today.
pub(in crate::application) struct PreparedDomainSchedule {
    pub(in crate::application) schedule: Option<DomainSchedule>,
    pub(in crate::application) relocations: usize,
}

#[derive(Clone, Copy)]
enum SchedulePlanningMode {
    Sticky,
    #[cfg(feature = "testing")]
    Random,
}

impl SchedulePlanningMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Sticky => "sticky",
            #[cfg(feature = "testing")]
            Self::Random => "random",
        }
    }

    #[cfg(feature = "testing")]
    const fn scheduler_mode(self) -> crate::registry::SchedulerMode {
        match self {
            Self::Sticky => crate::registry::SchedulerMode::Sticky,
            Self::Random => crate::registry::SchedulerMode::Random,
        }
    }
}

/// Immutable cluster inputs used by the synchronous schedule decision.
///
/// The control plane captures membership, liveness and eligibility once. A transaction planner can
/// then prepare every ordered step without observing a different cluster between steps.
#[derive(Clone)]
pub(in crate::application) struct DomainSchedulePlanningSnapshot {
    live_voters: Vec<ClusterNodeName>,
    cluster_nodes: Vec<ClusterNodeName>,
    replica_count: usize,
    mode: SchedulePlanningMode,
}

impl DomainSchedulePlanningSnapshot {
    pub(in crate::application) fn prepare(
        &self,
        domain: &DomainName,
        graph: Option<ActiveGraph>,
        placement: PlacementPolicy,
        current: Option<&DomainSchedule>,
    ) -> PreparedDomainSchedule {
        let schedule = match graph {
            Some(graph) => {
                #[cfg(feature = "testing")]
                let mut schedule = graph.schedule_for_domain_with_mode(
                    domain,
                    &self.cluster_nodes,
                    self.replica_count,
                    placement,
                    self.mode.scheduler_mode(),
                );
                #[cfg(not(feature = "testing"))]
                let mut schedule = graph.schedule_for_domain(
                    domain,
                    &self.cluster_nodes,
                    self.replica_count,
                    placement,
                );
                SessionServiceImpl::merge_existing_schedule_data(
                    &mut schedule,
                    current,
                    &self.live_voters,
                );
                prefer_former_owners_as_replicas(current, &mut schedule, &self.live_voters);
                Some(schedule)
            }
            None => None,
        };
        let relocations = planned_relocation_count(current, schedule.as_ref());
        PreparedDomainSchedule {
            schedule,
            relocations,
        }
    }

    pub(in crate::application) fn live_voters(&self) -> &[ClusterNodeName] {
        &self.live_voters
    }

    pub(in crate::application) fn cluster_nodes(&self) -> &[ClusterNodeName] {
        &self.cluster_nodes
    }

    pub(in crate::application) const fn replica_count(&self) -> usize {
        self.replica_count
    }

    pub(in crate::application) const fn mode_name(&self) -> &'static str {
        self.mode.as_str()
    }
}

impl SessionServiceImpl {
    pub(in crate::application) async fn capture_domain_schedule_planning_snapshot(
        &self,
    ) -> DomainSchedulePlanningSnapshot {
        let availability = self.inner.cluster.availability_state().await;
        let live_node_ids = availability.live_node_ids();
        let placement_candidate_node_ids = availability.placement_candidate_node_ids();
        let voters = self.inner.consensus.membership_voter_ids().await;
        let cordoned = self.inner.consensus.cordoned_node_ids().await;
        let mut live_voters = live_node_ids
            .into_iter()
            .filter(|node| voters.contains(node))
            .collect::<Vec<_>>();
        live_voters.sort();
        live_voters.dedup();
        let mut cluster_nodes = placement_candidate_node_ids
            .into_iter()
            .filter(|node| voters.contains(node) && !cordoned.contains(node))
            .collect::<Vec<_>>();
        cluster_nodes.sort();
        cluster_nodes.dedup();
        #[cfg(feature = "testing")]
        let mode = match self.inner.runtime.scheduler_mode() {
            crate::registry::SchedulerMode::Sticky => SchedulePlanningMode::Sticky,
            crate::registry::SchedulerMode::Random => SchedulePlanningMode::Random,
        };
        #[cfg(not(feature = "testing"))]
        let mode = SchedulePlanningMode::Sticky;
        DomainSchedulePlanningSnapshot {
            live_voters,
            cluster_nodes,
            replica_count: self.inner.replica_count,
            mode,
        }
    }
}
