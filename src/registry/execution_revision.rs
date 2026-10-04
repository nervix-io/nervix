//! Complete typed decisions for one committed domain schedule.
//!
//! Layer: decisions.
//! - **Owns.** Converting a validated schedule into node placement and all domain execution plans.
//! - **Depends on.** Scheduled vocabulary, expression lowering and the pure planning decisions.
//! - **Must not know.** Runtime tasks, node-local resources, locks or application progress.

use std::collections::BTreeMap;

use error_stack::{Report, ResultExt as _};
use indexmap::IndexMap;
use nervix_models::{
    ClusterNodeName, ClusterSchedule, DomainName, DomainSchedule, DynamicModelUpdate, FlushPolicy,
    KafkaPartitionSchedule, ModelKind, ModelName, NodeRef, OwnershipStateComponent,
    OwnershipTransition, ResolvedBranching, ScheduledNode, SchemaFingerprint, StatePurge,
    WasmStateGenerations, WasmStateReset,
};
use nervix_primitives::sync::Arc;
use thiserror::Error;

use super::{
    ActiveGraph, DomainActivationPlan, EntrypointPlans, MessageErrorRouteSpecs,
    ResourceExecutionPlans, ScheduleDelta, entity_pause_relays_for_schedule,
    processor_plan::{BranchedNodeSpecs, branched_node_specs_from_scheduled_nodes},
};
use crate::emitter_execution_plan::EmitterExecutionPlans;

#[derive(Debug, Error)]
pub(crate) enum ExecutionRevisionError {
    #[error("cannot plan domain '{domain}' activation")]
    Activation { domain: DomainName },
    #[error("cannot plan domain '{domain}' resources")]
    Resources { domain: DomainName },
    #[error("cannot plan domain '{domain}' entrypoints")]
    Entrypoints { domain: DomainName },
    #[error("cannot plan domain '{domain}' emitters")]
    Emitters { domain: DomainName },
    #[error("cannot plan domain '{domain}' message-error routes")]
    MessageErrors { domain: DomainName },
    #[error("cannot encode domain '{domain}' ownership schedule fingerprint")]
    Fingerprint { domain: DomainName },
    #[error("cannot encode domain '{domain}' execution revision identity")]
    RevisionIdentity { domain: DomainName },
}

/// Placement and state identity of one node. Its executable configuration has already been
/// converted into the domain's typed plans.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionNode {
    pub(crate) identifier: ModelName,
    pub(crate) identity: NodeRef,
    pub(crate) resolved_branching: Option<ResolvedBranching>,
    pub(crate) schema_fingerprint: SchemaFingerprint,
    pub(crate) kafka_partition_schedule: Option<KafkaPartitionSchedule>,
    pub(crate) primary_node: Option<ClusterNodeName>,
    pub(crate) assigned_nodes: Vec<ClusterNodeName>,
    pub(crate) ownership_transition: Option<OwnershipTransition>,
    pub(crate) materialized_relay: bool,
    pub(crate) ownership_state_components: Vec<OwnershipStateComponent>,
    pub(crate) gate_relays: Vec<nervix_models::RelayName>,
    pub(crate) wasm_state_generations: Option<WasmStateGenerations>,
    executes_on_every_cluster_node: bool,
}

impl ExecutionNode {
    pub(crate) fn from_scheduled(node: &ScheduledNode, schedule: &DomainSchedule) -> Self {
        let identity = node.identity();
        let materialized_relay = match node.config.as_ref() {
            nervix_models::Model::Relay(relay) => relay.materialized_state.is_some(),
            _ => false,
        };
        Self {
            identifier: node.identifier.clone(),
            identity,
            resolved_branching: node.resolved_branching.clone(),
            schema_fingerprint: node.schema_fingerprint,
            kafka_partition_schedule: node.kafka_partition_schedule.clone(),
            primary_node: node.primary_node.clone(),
            assigned_nodes: node.assigned_nodes.clone(),
            ownership_transition: node.ownership_transition.clone(),
            materialized_relay,
            ownership_state_components: node.ownership_state_components(),
            gate_relays: entity_pause_relays_for_schedule(
                schedule,
                std::slice::from_ref(&node.identity()),
            ),
            wasm_state_generations: node.wasm_state_generations().cloned(),
            executes_on_every_cluster_node: node.config.executes_on_every_cluster_node(),
        }
    }

    pub(crate) fn kind(&self) -> ModelKind {
        self.identity.kind
    }

    pub(crate) fn identity(&self) -> NodeRef {
        self.identity.clone()
    }

    pub(crate) fn wasm_state_generations(&self) -> Option<&WasmStateGenerations> {
        self.wasm_state_generations.as_ref()
    }

    pub(crate) fn ownership_state_components(&self) -> Vec<OwnershipStateComponent> {
        self.ownership_state_components.clone()
    }

    pub(crate) fn is_assigned_to(&self, node: &ClusterNodeName) -> bool {
        self.assigned_nodes.contains(node)
    }

    pub(crate) fn assigned_single_node(&self) -> Option<&ClusterNodeName> {
        match self.assigned_nodes.as_slice() {
            [node] => Some(node),
            _ => None,
        }
    }

    pub(crate) fn primary_node(&self) -> Option<&ClusterNodeName> {
        self.primary_node.as_ref()
    }

    pub(crate) fn replica_nodes(&self) -> Vec<&ClusterNodeName> {
        let primary = self.primary_node();
        self.assigned_nodes
            .iter()
            .filter(|node| Some(*node) != primary)
            .collect()
    }

    pub(crate) fn is_primary_on(&self, node: &ClusterNodeName) -> bool {
        match self.primary_node() {
            Some(primary) => primary == node,
            None => self.is_assigned_to(node),
        }
    }

    pub(crate) fn execution_node(&self) -> Option<&ClusterNodeName> {
        if self.executes_on_every_cluster_node {
            None
        } else {
            self.primary_node().or_else(|| self.assigned_single_node())
        }
    }

    pub(crate) fn executes_on(&self, node: &ClusterNodeName) -> bool {
        if self.executes_on_every_cluster_node {
            self.is_assigned_to(node)
        } else {
            self.is_primary_on(node)
        }
    }
}

/// A complete, in-memory revision; every plan and assignment comes from the same schedule.
pub(crate) struct ExecutionRevision {
    pub(crate) domain: DomainName,
    pub(crate) source_digest: [u8; 32],
    pub(crate) nodes: IndexMap<NodeRef, ExecutionNode>,
    pub(crate) activation: DomainActivationPlan,
    pub(crate) resources: ResourceExecutionPlans,
    pub(crate) entrypoints: EntrypointPlans,
    pub(crate) emitters: EmitterExecutionPlans,
    pub(crate) processors: BranchedNodeSpecs,
    pub(crate) message_errors: MessageErrorRouteSpecs,
    pub(crate) ownership_handoff_fingerprint: [u8; 32],
}

impl ExecutionRevision {
    pub(crate) fn from_graph(
        domain: &DomainName,
        graph: &ActiveGraph,
    ) -> error_stack::Result<Arc<Self>, ExecutionRevisionError> {
        let schedule =
            DomainSchedule::new(domain.clone(), graph.unplaced_schedule_nodes(), Vec::new());
        Self::from_schedule(&schedule)
    }

    pub(crate) fn from_schedule(
        schedule: &DomainSchedule,
    ) -> error_stack::Result<Arc<Self>, ExecutionRevisionError> {
        let domain = &schedule.domain;
        let activation = DomainActivationPlan::from_scheduled_nodes(domain, &schedule.nodes)
            .change_context_lazy(|| ExecutionRevisionError::Activation {
                domain: domain.clone(),
            })?;
        let resources =
            ResourceExecutionPlans::from_scheduled_nodes(domain, &schedule.nodes, &activation)
                .change_context_lazy(|| ExecutionRevisionError::Resources {
                    domain: domain.clone(),
                })?;
        let entrypoints =
            EntrypointPlans::from_scheduled_nodes(domain, &schedule.nodes, &activation)
                .change_context_lazy(|| ExecutionRevisionError::Entrypoints {
                    domain: domain.clone(),
                })?;
        let emitters = EmitterExecutionPlans::from_scheduled_nodes(&schedule.nodes, &activation)
            .change_context_lazy(|| ExecutionRevisionError::Emitters {
                domain: domain.clone(),
            })?;
        let processors = branched_node_specs_from_scheduled_nodes(&schedule.nodes);
        let message_errors =
            MessageErrorRouteSpecs::from_scheduled_nodes(domain, &schedule.nodes, &activation)
                .change_context_lazy(|| ExecutionRevisionError::MessageErrors {
                    domain: domain.clone(),
                })?;
        let ownership_handoff_fingerprint = Self::ownership_fingerprint(schedule)?;
        let source_digest = Self::source_digest(schedule)?;
        let nodes = schedule
            .nodes
            .iter()
            .map(|(identity, node)| {
                (
                    identity.clone(),
                    ExecutionNode::from_scheduled(node, schedule),
                )
            })
            .collect();
        Ok(Arc::new(Self {
            domain: domain.clone(),
            source_digest,
            nodes,
            activation,
            resources,
            entrypoints,
            emitters,
            processors,
            message_errors,
            ownership_handoff_fingerprint,
        }))
    }

    /// Keep the ownership-handoff bytes identical to the committed schedule's established form.
    pub(crate) fn ownership_fingerprint(
        schedule: &DomainSchedule,
    ) -> error_stack::Result<[u8; 32], ExecutionRevisionError> {
        #[derive(serde::Serialize)]
        struct ScheduledNodeFingerprint<'a> {
            identifier: &'a ModelName,
            config: &'a nervix_models::Model,
            resolved_branching: &'a Option<ResolvedBranching>,
            schema_fingerprint: SchemaFingerprint,
            kafka_partition_schedule: &'a Option<KafkaPartitionSchedule>,
            primary_node: &'a Option<ClusterNodeName>,
            assigned_nodes: &'a [ClusterNodeName],
            wasm_state_generations: Option<&'a WasmStateGenerations>,
            wasm_state_reset: Option<&'a WasmStateReset>,
        }

        #[derive(serde::Serialize)]
        struct DomainScheduleFingerprint<'a> {
            domain: &'a DomainName,
            nodes: Vec<ScheduledNodeFingerprint<'a>>,
            placement_groups: &'a [nervix_models::PlacementGroupSchedule],
        }

        let nodes = schedule
            .nodes
            .values()
            .map(|node| ScheduledNodeFingerprint {
                identifier: &node.identifier,
                config: node.config.as_ref(),
                resolved_branching: &node.resolved_branching,
                schema_fingerprint: node.schema_fingerprint,
                kafka_partition_schedule: &node.kafka_partition_schedule,
                primary_node: &node.primary_node,
                assigned_nodes: &node.assigned_nodes,
                wasm_state_generations: node.wasm_state_generations(),
                wasm_state_reset: node.wasm_state_reset(),
            })
            .collect();
        let fingerprint = DomainScheduleFingerprint {
            domain: &schedule.domain,
            nodes,
            placement_groups: &schedule.placement_groups,
        };
        let encoded = serde_json::to_vec(&fingerprint).map_err(|error| {
            Report::new(ExecutionRevisionError::Fingerprint {
                domain: schedule.domain.clone(),
            })
            .attach_printable(error)
        })?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix/ownership-handoff/domain-schedule");
        hasher.update(&encoded);
        Ok(*hasher.finalize().as_bytes())
    }

    fn source_digest(
        schedule: &DomainSchedule,
    ) -> error_stack::Result<[u8; 32], ExecutionRevisionError> {
        // JSON object keys cannot encode typed NodeRef values. Keep the complete keyed schedule
        // as an ordered sequence of pairs so both the identity and scheduled node are hashed.
        let nodes: Vec<_> = schedule.nodes.iter().collect();
        let encoded = serde_json::to_vec(&(&schedule.domain, nodes, &schedule.placement_groups))
            .map_err(|error| {
                Report::new(ExecutionRevisionError::RevisionIdentity {
                    domain: schedule.domain.clone(),
                })
                .attach_printable(error)
            })?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix/execution-revision/source-schedule");
        hasher.update(&encoded);
        Ok(*hasher.finalize().as_bytes())
    }
}

pub(crate) struct PlannedDomainChange {
    pub(crate) revision: Arc<ExecutionRevision>,
    pub(crate) predecessor_digest: Option<[u8; 32]>,
    pub(crate) delta: ExecutionDelta,
}

pub(crate) struct PlannedClusterRevision {
    pub(crate) domains: BTreeMap<DomainName, PlannedDomainChange>,
}

impl PlannedClusterRevision {
    pub(crate) fn between(
        existing: Option<&ClusterSchedule>,
        desired: &ClusterSchedule,
    ) -> error_stack::Result<Self, ExecutionRevisionError> {
        let mut domains = BTreeMap::new();
        for (domain, schedule) in &desired.domains {
            let previous = existing.and_then(|schedule| schedule.domain(domain));
            let revision = ExecutionRevision::from_schedule(schedule)?;
            let predecessor_digest = previous.map(ExecutionRevision::source_digest).transpose()?;
            let delta = ExecutionDelta::between(previous, Some(schedule));
            domains.insert(
                domain.clone(),
                PlannedDomainChange {
                    revision,
                    predecessor_digest,
                    delta,
                },
            );
        }
        Ok(Self { domains })
    }
}

/// The exact runtime changes chosen while both committed schedule shapes are still available in
/// the decision layer. No executable Model crosses the application boundary.
pub(crate) enum ExecutionDelta {
    Unchanged,
    Dynamic(Vec<DynamicExecutionUpdate>),
    EntitySwap(EntitySwapExecution),
    Rebuild,
}

pub(crate) struct EntitySwapExecution {
    pub(crate) entities: Vec<NodeRef>,
    pub(crate) reassignments: Vec<NodeRef>,
    pub(crate) dynamic_updates: Vec<DynamicExecutionUpdate>,
    pub(crate) state_purges: BTreeMap<NodeRef, Vec<StatePurge>>,
    pub(crate) gate_relays: Vec<nervix_models::RelayName>,
}

pub(crate) enum DynamicExecutionUpdate {
    RelayCapacity {
        relay: nervix_models::RelayName,
        capacity: std::num::NonZeroUsize,
    },
    Processor,
    WasmStateReset {
        processor: ModelName,
        reset: WasmStateReset,
    },
    EmitterFlush {
        emitter: nervix_models::EmitterName,
        policy: FlushPolicy,
    },
    VhostTlsVersion,
}

impl From<DynamicModelUpdate> for DynamicExecutionUpdate {
    fn from(update: DynamicModelUpdate) -> Self {
        match update {
            DynamicModelUpdate::RelayCapacity { relay, capacity } => {
                Self::RelayCapacity { relay, capacity }
            }
            DynamicModelUpdate::Processor { .. } => Self::Processor,
            DynamicModelUpdate::WasmStateReset { processor, reset } => {
                Self::WasmStateReset { processor, reset }
            }
            DynamicModelUpdate::Emitter { emitter, config } => Self::EmitterFlush {
                emitter,
                policy: config.flush_policy.clone(),
            },
            DynamicModelUpdate::VhostTlsVersion { .. } => Self::VhostTlsVersion,
        }
    }
}

impl ExecutionDelta {
    pub(crate) fn between(
        existing: Option<&DomainSchedule>,
        desired: Option<&DomainSchedule>,
    ) -> Self {
        match ScheduleDelta::between(existing, desired) {
            ScheduleDelta::Unchanged => Self::Unchanged,
            ScheduleDelta::Dynamic(updates) => {
                Self::Dynamic(updates.into_iter().map(Into::into).collect())
            }
            ScheduleDelta::EntitySwap {
                entities,
                reassignments,
                dynamic_updates,
            } => {
                let mut state_purges = BTreeMap::new();
                if let (Some(existing), Some(desired)) = (existing, desired) {
                    for entity in &entities {
                        if let (Some(before), Some(after)) =
                            (existing.nodes.get(entity), desired.nodes.get(entity))
                        {
                            state_purges.insert(
                                entity.clone(),
                                before
                                    .config
                                    .change_aspects_against(&after.config)
                                    .state_purges(),
                            );
                        }
                    }
                }
                let gate_relays = if let Some(schedule) = desired {
                    let mut gated = entities.clone();
                    gated.extend(reassignments.iter().cloned());
                    entity_pause_relays_for_schedule(schedule, &gated)
                } else {
                    Vec::new()
                };
                Self::EntitySwap(EntitySwapExecution {
                    entities,
                    reassignments,
                    dynamic_updates: dynamic_updates.into_iter().map(Into::into).collect(),
                    state_purges,
                    gate_relays,
                })
            }
            ScheduleDelta::Rebuild => Self::Rebuild,
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{CreateRelay, CreateSchema, Model, RelayBranching, SchemaField};
    use nonzero_ext::nonzero;

    use super::*;
    use crate::registry::test_fixtures::named;

    fn schedule(capacity: std::num::NonZeroUsize) -> DomainSchedule {
        let fingerprint = SchemaFingerprint::from_digest([7; 32]);
        let schema = ScheduledNode::new(
            Model::Schema(CreateSchema {
                name: named("event"),
                fields: vec![SchemaField {
                    name: named("value"),
                    ty: nervix_models::ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            }),
            fingerprint,
        );
        let relay = ScheduledNode::new(
            Model::Relay(CreateRelay {
                name: named("events"),
                schema: named("event"),
                buffer: capacity,
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            }),
            fingerprint,
        )
        .with_resolved_branching(Some(ResolvedBranching::unbranched()));
        DomainSchedule::new(named("testing"), vec![schema, relay], Vec::new())
    }

    #[test]
    fn complete_revision_keeps_plans_placement_and_handoff_identity_together() {
        let mut schedule = schedule(nonzero!(2usize));
        let owner = named::<ClusterNodeName>("node-1");
        let relay = NodeRef::new(ModelKind::Relay, named::<ModelName>("events"));
        let node = schedule
            .nodes
            .get_mut(&relay)
            .assured("the fixture schedules its relay");
        node.primary_node = Some(owner.clone());
        node.assigned_nodes = vec![owner.clone()];

        let revision = ExecutionRevision::from_schedule(&schedule)
            .assured("a schema and its relay form one complete execution revision");
        let placed = revision
            .nodes
            .get(&relay)
            .assured("the revision keeps the relay placement");
        assert!(placed.executes_on(&owner));
        assert_eq!(placed.execution_node(), Some(&owner));
        assert_eq!(
            placed.schema_fingerprint,
            SchemaFingerprint::from_digest([7; 32])
        );
        assert!(
            revision
                .activation
                .relays
                .contains_key(&named::<nervix_models::RelayName>("events"))
        );
        assert_eq!(
            revision.ownership_handoff_fingerprint,
            ExecutionRevision::ownership_fingerprint(&schedule)
                .assured("the committed schedule has a stable handoff fingerprint")
        );
    }

    #[test]
    fn cluster_revision_classifies_changes_from_its_applied_predecessor() {
        let before = schedule(nonzero!(2usize));
        let after = schedule(nonzero!(4usize));
        let previous = ClusterSchedule::from_iter([before]);
        let desired = ClusterSchedule::from_iter([after]);
        let plan = PlannedClusterRevision::between(Some(&previous), &desired)
            .assured("the changed relay has complete plans");
        let change = plan
            .domains
            .get(&named::<DomainName>("testing"))
            .assured("the cluster revision includes the changed domain");
        assert_eq!(
            change.predecessor_digest,
            Some(
                ExecutionRevision::source_digest(
                    previous
                        .domain(&named("testing"))
                        .assured("the predecessor has the domain")
                )
                .assured("the predecessor has a revision identity")
            )
        );
        assert_ne!(
            change.predecessor_digest,
            Some(change.revision.source_digest)
        );
        assert!(matches!(&change.delta,
            ExecutionDelta::Dynamic(updates)
                if matches!(updates.as_slice(), [DynamicExecutionUpdate::RelayCapacity { relay, capacity }]
                    if relay == &named("events") && *capacity == nonzero!(4usize))));
        let same = PlannedClusterRevision::between(Some(&desired), &desired)
            .assured("the same committed revision remains plannable");
        assert!(matches!(
            same.domains
                .get(&named::<DomainName>("testing"))
                .assured("the unchanged domain is present")
                .delta,
            ExecutionDelta::Unchanged
        ));
    }

    #[test]
    fn relay_reassignment_is_a_typed_entity_swap() {
        let mut before = schedule(nonzero!(2usize));
        let relay = NodeRef::new(ModelKind::Relay, named::<ModelName>("events"));
        let source = named::<ClusterNodeName>("node-1");
        let destination = named::<ClusterNodeName>("node-2");
        let node = before
            .nodes
            .get_mut(&relay)
            .assured("the fixture schedules its relay");
        node.primary_node = Some(source.clone());
        node.assigned_nodes = vec![source];
        let mut after = before.clone();
        let node = after
            .nodes
            .get_mut(&relay)
            .assured("the fixture schedules its relay");
        node.primary_node = Some(destination.clone());
        node.assigned_nodes = vec![destination];

        assert!(
            matches!(ExecutionDelta::between(Some(&before), Some(&after)),
            ExecutionDelta::EntitySwap(change) if change.reassignments == vec![relay])
        );
    }

    #[test]
    fn a_missing_schema_fails_before_runtime_installation() {
        let complete = schedule(nonzero!(2usize));
        let relay = complete
            .nodes
            .values()
            .find(|node| node.kind() == ModelKind::Relay)
            .cloned()
            .assured("the fixture schedules its relay");
        let incomplete = DomainSchedule::new(complete.domain, vec![relay], Vec::new());
        let error = ExecutionRevision::from_schedule(&incomplete)
            .err()
            .assured("an incomplete domain cannot become an execution revision");
        assert!(matches!(
            error.current_context(),
            ExecutionRevisionError::Activation { .. }
        ));
    }
}
