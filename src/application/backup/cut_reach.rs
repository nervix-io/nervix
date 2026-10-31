//! The nodes one domain cut must reach.
//!
//! Layer: control plane.
//!
//! - **Owns.** Deriving, from a domain's committed schedule, the scheduled primary of every
//!   placement whose state a backup archives, every node the schedule assigns the domain's work
//!   to, and the first entity whose state the archive captures but which the schedule places on no
//!   node, and naming the first required node that the cluster does not report live.
//! - **Depends on.** The vocabulary's domain schedule, graph-node references and cluster node
//!   names.
//! - **Must not know.** How a node is asked to drain or capture, or what its state holds.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::{BTreeMap, BTreeSet};

use nervix_models::{ClusterNodeName, DomainSchedule, NodeRef, OwnershipStateComponent};

/// A node a cut must reach, with the first entity of the schedule that requires it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct RequiredNode {
    pub(in crate::application) node: ClusterNodeName,
    pub(in crate::application) entity: NodeRef,
}

/// The nodes one cut of a domain must reach, derived from the domain's committed schedule.
#[derive(Debug, Default)]
pub(in crate::application) struct CutReach {
    state_owners: RequiredNodes,
    executors: RequiredNodes,
    /// The first entity, in the order the schedule places them, whose state the archive captures
    /// but which the schedule places on no node, as when no node can host it.
    unplaced_state: Option<NodeRef>,
}

impl CutReach {
    /// What a cut of the domain `schedule` places must reach. A domain without a schedule places
    /// nothing and requires no node.
    pub(in crate::application) fn of(schedule: Option<&DomainSchedule>) -> Self {
        let mut reach = Self::default();
        let Some(schedule) = schedule else {
            return reach;
        };
        for (entity, scheduled) in &schedule.nodes {
            for unit in scheduled.units.values() {
                for candidate in unit.primary_node.iter().chain(&unit.assigned_nodes) {
                    if (scheduled.config.executes_on_every_cluster_node()
                        && unit.assigned_nodes.contains(candidate))
                        || unit.is_primary_on(candidate)
                    {
                        reach.executors.require(candidate, entity);
                    }
                }
            }
            let components = scheduled.ownership_state_components();
            if !components
                .into_iter()
                .any(OwnershipStateComponent::is_archived)
            {
                continue;
            }
            // Only a placement's scheduled primary stages its state, so the primary is the one
            // node whose absence leaves the archive without it. An entity the schedule places on
            // no node has no owner a cut could ask, while its last state may still lie on a node.
            for unit in scheduled.units.values() {
                match unit.primary_node.as_ref() {
                    Some(owner) => reach.state_owners.require(owner, entity),
                    None => {
                        if reach.unplaced_state.is_none() {
                            reach.unplaced_state = Some(entity.clone());
                        }
                    }
                }
            }
        }
        reach
    }

    /// The first entity whose state the cut archives but which the schedule places on no node.
    pub(in crate::application) fn unplaced_state(&self) -> Option<&NodeRef> {
        self.unplaced_state.as_ref()
    }

    /// The scheduled primaries of the placements whose state the cut archives, by name.
    pub(in crate::application) fn state_owners(&self) -> impl Iterator<Item = &ClusterNodeName> {
        self.state_owners.nodes()
    }

    /// The nodes the schedule assigns the domain's work to, by name.
    pub(in crate::application) fn executors(&self) -> impl Iterator<Item = &ClusterNodeName> {
        self.executors.nodes()
    }

    /// The first state owner, by name, that `live` does not hold.
    pub(in crate::application) fn unreached_state_owner(
        &self,
        live: &BTreeSet<ClusterNodeName>,
    ) -> Option<RequiredNode> {
        self.state_owners.first_outside(live)
    }

    /// The first executor, by name, that `live` does not hold.
    pub(in crate::application) fn unreached_executor(
        &self,
        live: &BTreeSet<ClusterNodeName>,
    ) -> Option<RequiredNode> {
        self.executors.first_outside(live)
    }
}

/// Nodes a cut requires, each with the first entity, in the order the schedule places them, that
/// requires it, so a refusal names the same entity whenever the cut is refused for that node.
#[derive(Debug, Default)]
struct RequiredNodes(BTreeMap<ClusterNodeName, NodeRef>);

impl RequiredNodes {
    fn require(&mut self, node: &ClusterNodeName, entity: &NodeRef) {
        self.0.entry(node.clone()).or_insert_with(|| entity.clone());
    }

    fn nodes(&self) -> impl Iterator<Item = &ClusterNodeName> {
        self.0.keys()
    }

    fn first_outside(&self, live: &BTreeSet<ClusterNodeName>) -> Option<RequiredNode> {
        for (node, entity) in &self.0 {
            if !live.contains(node) {
                return Some(RequiredNode {
                    node: node.clone(),
                    entity: entity.clone(),
                });
            }
        }
        None
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::collections::BTreeSet;

    use nervix_models::{
        ClusterNodeName, CreateIngestor, CreateRelay, DomainName, DomainSchedule,
        EndpointIngestMode, GeneralErrorPolicy, HttpEndpointIngestor, HttpIngestor,
        IngestQuiesceMode, IngestorEntity, IngestorInput, MaterializedRelayState, Model, ModelKind,
        NodeRef, ProcessorOutputs, RelayBranching, ScheduledNode, SchemaFingerprint,
        TransportIngestorInput, Unit,
    };
    use nonzero_ext::nonzero;

    use super::{CutReach, RequiredNode};
    use crate::application::test_fixtures::{named, node_named, scheduled_node};

    fn domain_schedule(nodes: Vec<ScheduledNode>) -> DomainSchedule {
        DomainSchedule::new(named::<DomainName>("payments"), nodes, Vec::new())
    }

    fn placed(node: ScheduledNode, primary: Option<&str>, assigned: &[&str]) -> ScheduledNode {
        let assigned = assigned.iter().map(|raw| node_named(raw)).collect();
        node.placed_on(primary.map(node_named), assigned)
    }

    fn relay(raw: &str, materialized_state: Option<MaterializedRelayState>) -> ScheduledNode {
        ScheduledNode::new(
            Model::Relay(CreateRelay {
                name: named(raw),
                schema: named("payment"),
                buffer: nonzero!(16usize),
                branching: RelayBranching::unbranched(),
                materialized_state,
            }),
            SchemaFingerprint::from_digest([2; 32]),
        )
    }

    /// An HTTP endpoint ingestor, which executes on every node the schedule assigns it to.
    fn endpoint_ingestor(raw: &str) -> ScheduledNode {
        ScheduledNode::new(
            Model::Ingestor(CreateIngestor {
                name: named(raw),
                output_routes: ProcessorOutputs::new(Vec::new()),
                input: IngestorInput::Transport(TransportIngestorInput {
                    source: IngestorEntity::Http(HttpIngestor::Endpoint(HttpEndpointIngestor {
                        endpoint: named("payment_ingress"),
                        mode: EndpointIngestMode::NoAck,
                        quiesce: IngestQuiesceMode::EndpointBuffer {
                            max_size: "1MiB".to_string(),
                        },
                    })),
                    codec: named("payment_codec"),
                }),
                timestamp_source: None,
                general_error_policy: GeneralErrorPolicy::Log,
                filter_where: None,
            }),
            SchemaFingerprint::from_digest([3; 32]),
        )
    }

    fn live(nodes: &[&str]) -> BTreeSet<ClusterNodeName> {
        nodes.iter().map(|raw| node_named(raw)).collect()
    }

    fn entity(kind: ModelKind, raw: &str) -> NodeRef {
        NodeRef::new(kind, named::<nervix_models::ModelName>(raw))
    }

    fn names<'node>(nodes: impl Iterator<Item = &'node ClusterNodeName>) -> Vec<&'node str> {
        nodes.map(ClusterNodeName::as_str).collect()
    }

    #[test]
    fn a_state_owner_is_the_scheduled_primary_and_never_a_replica() {
        let schedule = domain_schedule(vec![placed(
            scheduled_node("unique_payment_filter", ModelKind::Deduplicator),
            Some("node-2"),
            &["node-2", "node-3"],
        )]);

        let reach = CutReach::of(Some(&schedule));

        assert_eq!(names(reach.state_owners()), vec!["node-2"]);
        assert_eq!(names(reach.executors()), vec!["node-2"]);
        assert_eq!(
            reach.unreached_state_owner(&live(&["node-1", "node-3"])),
            Some(RequiredNode {
                node: node_named("node-2"),
                entity: entity(ModelKind::Deduplicator, "unique_payment_filter"),
            })
        );
        assert_eq!(
            reach.unreached_state_owner(&live(&["node-1", "node-2"])),
            None
        );
    }

    #[test]
    fn each_partition_primary_is_required_for_a_complete_cut() {
        let mut node = placed(
            scheduled_node("unique_payment_filter", ModelKind::Deduplicator),
            Some("node-1"),
            &["node-1", "node-3"],
        );
        let first = node.single_unit().clone();
        node.units.clear();
        node.units.insert(Unit::Partition(0), first.clone());
        node.units.insert(
            Unit::Partition(1),
            nervix_models::ScheduledUnit {
                primary_node: Some(node_named("node-2")),
                assigned_nodes: vec![node_named("node-2"), node_named("node-3")],
                ..first
            },
        );
        let reach = CutReach::of(Some(&domain_schedule(vec![node])));

        assert_eq!(names(reach.state_owners()), vec!["node-1", "node-2"]);
        assert_eq!(names(reach.executors()), vec!["node-1", "node-2"]);
    }

    #[test]
    fn an_every_node_listener_runs_on_every_assigned_node_and_archives_through_its_primary() {
        let schedule = domain_schedule(vec![placed(
            endpoint_ingestor("payment_source"),
            Some("node-1"),
            &["node-1", "node-2", "node-3"],
        )]);

        let reach = CutReach::of(Some(&schedule));

        assert_eq!(names(reach.executors()), vec!["node-1", "node-2", "node-3"]);
        assert_eq!(names(reach.state_owners()), vec!["node-1"]);
        assert_eq!(
            reach.unreached_executor(&live(&["node-1", "node-2"])),
            Some(RequiredNode {
                node: node_named("node-3"),
                entity: entity(ModelKind::Ingestor, "payment_source"),
            })
        );
        assert_eq!(
            reach.unreached_state_owner(&live(&["node-1", "node-2"])),
            None
        );
    }

    #[test]
    fn work_without_archived_state_requires_its_executor_and_no_state_owner() {
        let schedule = domain_schedule(vec![
            placed(relay("payments", None), Some("node-2"), &["node-2"]),
            placed(
                scheduled_node("published", ModelKind::Emitter),
                Some("node-3"),
                &["node-3"],
            ),
        ]);

        let reach = CutReach::of(Some(&schedule));

        assert_eq!(names(reach.executors()), vec!["node-2", "node-3"]);
        assert_eq!(names(reach.state_owners()), Vec::<&str>::new());
    }

    #[test]
    fn a_materialized_relay_and_kafka_domain_offsets_are_archived_state() {
        let schedule = domain_schedule(vec![
            placed(
                relay("balances", Some(MaterializedRelayState::LastByTimestamp)),
                Some("node-3"),
                &["node-3", "node-1"],
            ),
            placed(
                scheduled_node("notifications_source", ModelKind::Ingestor),
                Some("node-2"),
                &["node-2"],
            ),
        ]);

        let reach = CutReach::of(Some(&schedule));

        assert_eq!(names(reach.state_owners()), vec!["node-2", "node-3"]);
    }

    #[test]
    fn a_refusal_names_the_first_entity_the_schedule_places_on_the_node() {
        let schedule = domain_schedule(vec![
            placed(
                scheduled_node("guest", ModelKind::WasmProcessor),
                Some("node-2"),
                &["node-2"],
            ),
            placed(
                scheduled_node("unique_payment_filter", ModelKind::Deduplicator),
                Some("node-2"),
                &["node-2"],
            ),
        ]);

        let reach = CutReach::of(Some(&schedule));

        assert_eq!(
            reach.unreached_state_owner(&live(&["node-1"])),
            Some(RequiredNode {
                node: node_named("node-2"),
                entity: entity(ModelKind::WasmProcessor, "guest"),
            })
        );
    }

    #[test]
    fn a_state_bearing_entry_placed_on_no_node_is_reported_and_requires_no_node() {
        let unplaced = domain_schedule(vec![
            placed(relay("payments", None), None, &[]),
            placed(
                scheduled_node("unique_payment_filter", ModelKind::Deduplicator),
                None,
                &[],
            ),
            placed(scheduled_node("guest", ModelKind::WasmProcessor), None, &[]),
        ]);

        let reach = CutReach::of(Some(&unplaced));

        assert_eq!(
            reach.unplaced_state(),
            Some(&entity(ModelKind::Deduplicator, "unique_payment_filter"))
        );
        assert_eq!(names(reach.state_owners()), Vec::<&str>::new());
        assert_eq!(names(reach.executors()), Vec::<&str>::new());
    }

    #[test]
    fn an_absent_schedule_and_unplaced_work_without_state_require_nothing() {
        let unplaced = domain_schedule(vec![
            placed(relay("payments", None), None, &[]),
            placed(scheduled_node("published", ModelKind::Emitter), None, &[]),
        ]);

        for reach in [CutReach::of(Some(&unplaced)), CutReach::of(None)] {
            assert_eq!(names(reach.state_owners()), Vec::<&str>::new());
            assert_eq!(names(reach.executors()), Vec::<&str>::new());
            assert_eq!(reach.unplaced_state(), None);
            assert_eq!(reach.unreached_state_owner(&live(&[])), None);
            assert_eq!(reach.unreached_executor(&live(&[])), None);
        }
    }
}
