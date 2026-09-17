//! Immutable cluster inputs for synchronous domain-schedule planning.
//!
//! Layer: control plane.
//!
//! - **Owns.** Capturing one membership, liveness, eligibility and scheduler-policy snapshot.
//! - **Depends on.** Cluster observation, consensus topology and the registry scheduler decision.
//! - **Must not know.** Transaction syntax, persistence, runtime activation or commit progress.

use std::collections::BTreeSet;

use error_stack::Report;
use nervix_consensus::DomainPlanningInputs;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeName, DomainName, DomainSchedule, PlacementPolicy,
};
use sorted_vec::SortedSet;
use thiserror::Error;

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
    pub(in crate::application) inputs: DomainPlanningInputs,
    pub(in crate::application) planning: DomainSchedulePlanningSnapshot,
}

#[derive(Debug, Error)]
pub(in crate::application) enum SchedulePlanningStale {
    #[error("domain '{}' configuration changed", .domain.as_str())]
    Domain { domain: DomainName },
    #[error("domain '{}' resource inputs changed", .domain.as_str())]
    Resources { domain: DomainName },
    #[error("domain '{}' schedule changed", .domain.as_str())]
    Schedule { domain: DomainName },
    #[error("domain '{}' scheduling topology changed", .domain.as_str())]
    Topology { domain: DomainName },
    #[error("domain '{}' scheduling eligibility changed while the plan was pending", .domain.as_str())]
    Eligibility { domain: DomainName },
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
    domain: DomainName,
    voters: SortedSet<ClusterNodeName>,
    live_identities: BTreeSet<ClusterNodeIdentity>,
    placement_candidate_identities: BTreeSet<ClusterNodeIdentity>,
    live_voters: SortedSet<ClusterNodeName>,
    cluster_nodes: SortedSet<ClusterNodeName>,
    replica_count: usize,
    mode: SchedulePlanningMode,
}

impl DomainSchedulePlanningSnapshot {
    fn eligibility_inputs(
        availability: &nervix_consensus::GossipState,
        voters: &[ClusterNodeName],
    ) -> (BTreeSet<ClusterNodeIdentity>, BTreeSet<ClusterNodeIdentity>) {
        let live_identities = availability
            .live_identities()
            .into_iter()
            .filter(|identity| voters.contains(identity.node_id()))
            .collect::<BTreeSet<_>>();
        let placement_candidates = availability.placement_candidate_node_ids();
        let placement_candidate_identities = live_identities
            .iter()
            .filter(|identity| placement_candidates.contains(identity.node_id()))
            .cloned()
            .collect();
        (live_identities, placement_candidate_identities)
    }

    pub(in crate::application) fn same_eligibility(
        planned: &nervix_consensus::GossipState,
        current: &nervix_consensus::GossipState,
        voters: &[ClusterNodeName],
    ) -> bool {
        Self::eligibility_inputs(planned, voters) == Self::eligibility_inputs(current, voters)
    }

    pub(in crate::application) fn prepare(
        &self,
        inputs: &DomainPlanningInputs,
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
            inputs: inputs.clone(),
            planning: self.clone(),
        }
    }

    pub(in crate::application) fn live_voters(&self) -> &[ClusterNodeName] {
        &self.live_voters
    }

    pub(in crate::application) fn cluster_nodes(&self) -> &[ClusterNodeName] {
        &self.cluster_nodes
    }

    pub(in crate::application) fn live_node_ids(&self) -> BTreeSet<ClusterNodeName> {
        self.live_identities
            .iter()
            .map(|identity| identity.node_id().clone())
            .collect()
    }

    pub(in crate::application) fn placement_candidate_node_ids(&self) -> BTreeSet<ClusterNodeName> {
        self.placement_candidate_identities
            .iter()
            .map(|identity| identity.node_id().clone())
            .collect()
    }

    pub(in crate::application) fn live_identities(&self) -> &BTreeSet<ClusterNodeIdentity> {
        &self.live_identities
    }

    pub(in crate::application) fn placement_candidate_identities(
        &self,
    ) -> &BTreeSet<ClusterNodeIdentity> {
        &self.placement_candidate_identities
    }

    pub(in crate::application) const fn replica_count(&self) -> usize {
        self.replica_count
    }

    pub(in crate::application) const fn mode_name(&self) -> &'static str {
        self.mode.as_str()
    }

    /// Recheck the volatile liveness and process-incarnation inputs consumed by this plan.
    pub(in crate::application) async fn validate_eligibility(
        &self,
        service: &SessionServiceImpl,
    ) -> error_stack::Result<(), SchedulePlanningStale> {
        let availability = service.inner.cluster.availability_state().await;
        let (live_identities, placement_candidate_identities) =
            Self::eligibility_inputs(&availability, &self.voters);
        if self.live_identities != live_identities
            || self.placement_candidate_identities != placement_candidate_identities
        {
            return Err(Report::new(SchedulePlanningStale::Eligibility {
                domain: self.domain.clone(),
            }));
        }
        Ok(())
    }
}

impl SessionServiceImpl {
    pub(in crate::application) async fn validate_domain_planning_inputs(
        &self,
        expected: &DomainPlanningInputs,
    ) -> error_stack::Result<(), SchedulePlanningStale> {
        let current = self
            .inner
            .consensus
            .domain_planning_inputs(expected.domain())
            .await;
        if current.state() != expected.state() {
            return Err(Report::new(SchedulePlanningStale::Domain {
                domain: expected.domain().clone(),
            }));
        }
        if current.resources() != expected.resources() {
            return Err(Report::new(SchedulePlanningStale::Resources {
                domain: expected.domain().clone(),
            }));
        }
        if current.schedule() != expected.schedule() {
            return Err(Report::new(SchedulePlanningStale::Schedule {
                domain: expected.domain().clone(),
            }));
        }
        if current.topology() != expected.topology() {
            return Err(Report::new(SchedulePlanningStale::Topology {
                domain: expected.domain().clone(),
            }));
        }
        Ok(())
    }

    pub(in crate::application) async fn capture_domain_schedule_planning_snapshot(
        &self,
        inputs: &DomainPlanningInputs,
    ) -> DomainSchedulePlanningSnapshot {
        let availability = self.inner.cluster.availability_state().await;
        let voters: SortedSet<ClusterNodeName> =
            inputs.topology().voters().iter().cloned().collect();
        let (live_identities, placement_candidate_identities) =
            DomainSchedulePlanningSnapshot::eligibility_inputs(&availability, &voters);
        let live_node_ids = availability.live_node_ids();
        let placement_candidate_node_ids = availability.placement_candidate_node_ids();
        let cordoned = inputs.topology().cordoned();
        let live_voters: SortedSet<ClusterNodeName> = live_node_ids
            .into_iter()
            .filter(|node| voters.contains(node))
            .collect();
        let cluster_nodes: SortedSet<ClusterNodeName> = placement_candidate_node_ids
            .into_iter()
            .filter(|node| voters.contains(node) && !cordoned.contains(node))
            .collect();
        #[cfg(feature = "testing")]
        let mode = match self.inner.runtime.scheduler_mode() {
            crate::registry::SchedulerMode::Sticky => SchedulePlanningMode::Sticky,
            crate::registry::SchedulerMode::Random => SchedulePlanningMode::Random,
        };
        #[cfg(not(feature = "testing"))]
        let mode = SchedulePlanningMode::Sticky;
        DomainSchedulePlanningSnapshot {
            domain: inputs.domain().clone(),
            voters,
            live_identities,
            placement_candidate_identities,
            live_voters,
            cluster_nodes,
            replica_count: self.inner.replica_count,
            mode,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use meticulous::ResultExt as _;
    use nervix_consensus::{GossipNode, GossipState};
    use nervix_models::{ClusterNodeIncarnation, ClusterNodeName};

    use super::DomainSchedulePlanningSnapshot;

    fn gossip_node(name: &str, incarnation: u64, terminating: bool) -> GossipNode {
        GossipNode {
            node_id: ClusterNodeName::parse(name).assured("the test node name is valid"),
            incarnation: ClusterNodeIncarnation::new(incarnation),
            terminating,
            grpc_advertise_addr: String::new(),
            web_console_advertise_addr: String::new(),
            interconnect_advertise_addr: String::new(),
        }
    }

    #[test]
    fn schedule_eligibility_tracks_voter_termination_and_incarnation_only() {
        let voter = ClusterNodeName::parse("node-1").assured("the test node name is valid");
        let voters = [voter];
        let planned = GossipState {
            live_nodes: vec![gossip_node("node-1", 1, false)],
            dead_node_ids: BTreeSet::new(),
        };
        let terminating = GossipState {
            live_nodes: vec![gossip_node("node-1", 1, true)],
            dead_node_ids: BTreeSet::new(),
        };
        let restarted = GossipState {
            live_nodes: vec![gossip_node("node-1", 2, false)],
            dead_node_ids: BTreeSet::new(),
        };
        let unrelated = GossipState {
            live_nodes: vec![
                gossip_node("node-1", 1, false),
                gossip_node("learner", 1, true),
            ],
            dead_node_ids: BTreeSet::new(),
        };

        assert!(!DomainSchedulePlanningSnapshot::same_eligibility(
            &planned,
            &terminating,
            &voters,
        ));
        assert!(!DomainSchedulePlanningSnapshot::same_eligibility(
            &planned, &restarted, &voters,
        ));
        assert!(DomainSchedulePlanningSnapshot::same_eligibility(
            &planned, &unrelated, &voters,
        ));
    }
}
