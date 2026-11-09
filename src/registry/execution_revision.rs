//! Complete typed decisions for one committed domain schedule.
//!
//! Layer: decisions.
//! - **Owns.** Converting a validated schedule into node placement and all domain execution plans.
//! - **Depends on.** Scheduled vocabulary, expression lowering and the pure planning decisions.
//! - **Must not know.** Runtime tasks, node-local resources, locks or application progress.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::collections::{BTreeMap, BTreeSet};

use error_stack::{Report, ResultExt as _};
use indexmap::IndexMap;
use nervix_models::{
    BranchName, BranchPartitionEpoch, ClusterNodeName, ClusterSchedule, DomainName, DomainSchedule,
    DynamicModelUpdate, FlushPolicy, KafkaPartitionSchedule, ModelKind, ModelName, NodeRef,
    NodeUnitRef, OwnershipStateComponent, OwnershipStateRecoveryOutcome, OwnershipTransition,
    ResolvedBranching, ScheduledNode, SchemaFingerprint, StatePurge, Unit, WasmStateGenerations,
    WasmStateReset,
};
use nervix_primitives::sync::Arc;
use thiserror::Error;

#[cfg(test)]
use super::ActiveGraph;
use super::{
    DomainActivationPlan, EntrypointPlans, MessageErrorRouteSpecs, ResourceExecutionPlans,
    ScheduleDelta, entity_pause_relays_for_schedule,
    processor_plan::{BranchedNodeSpecs, branched_node_specs_from_scheduled_nodes},
};
use crate::emitter_execution_plan::EmitterExecutionPlans;

#[derive(Debug, Error)]
pub(crate) enum ExecutionRevisionError {
    #[error(
        "scheduled {node:?} in domain '{domain}' has an invalid unit declaration; recreate its \
         schedule"
    )]
    UnitDeclaration { domain: DomainName, node: NodeRef },
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

/// Placement and guest-state lifetime of one execution unit in a typed revision.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionUnit {
    pub(crate) primary_node: Option<ClusterNodeName>,
    pub(crate) assigned_nodes: Vec<ClusterNodeName>,
    pub(crate) ownership_transition: Option<OwnershipTransition>,
    pub(crate) wasm_state_generations: Option<WasmStateGenerations>,
    pub(crate) wasm_state_reset: Option<nervix_models::WasmStateReset>,
}

impl ExecutionUnit {
    pub(crate) fn execution_node(&self) -> Option<&ClusterNodeName> {
        self.primary_node
            .as_ref()
            .or(match self.assigned_nodes.as_slice() {
                [node] => Some(node),
                _ => None,
            })
    }

    pub(crate) fn replica_nodes(&self) -> Vec<&ClusterNodeName> {
        self.assigned_nodes
            .iter()
            .filter(|node| Some(*node) != self.primary_node.as_ref())
            .collect()
    }

    fn is_assigned_to(&self, node: &ClusterNodeName) -> bool {
        self.assigned_nodes.contains(node)
    }

    fn is_primary_on(&self, node: &ClusterNodeName) -> bool {
        match &self.primary_node {
            Some(primary) => primary == node,
            None => self.is_assigned_to(node),
        }
    }
}

/// State identity of one logical node and its independently placed units. Its executable
/// configuration has already been converted into the domain's typed plans.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionNode {
    pub(crate) identifier: ModelName,
    pub(crate) identity: NodeRef,
    pub(crate) resolved_branching: Option<ResolvedBranching>,
    pub(crate) schema_fingerprint: SchemaFingerprint,
    pub(crate) kafka_partition_schedule: Option<KafkaPartitionSchedule>,
    pub(crate) units: IndexMap<Unit, ExecutionUnit>,
    pub(crate) materialized_relay: bool,
    pub(crate) ownership_state_components: Vec<OwnershipStateComponent>,
    pub(crate) gate_relays: Vec<nervix_models::RelayName>,
    pub(crate) executes_on_every_cluster_node: bool,
    /// The transition this node's branch is in while keys placed under its previous partition
    /// count may lie outside their partition, for a node that keeps state per key.
    pub(crate) partition_transition: Option<NodePartitionTransition>,
}

/// The partition transition one node that keeps state per key executes in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodePartitionTransition {
    pub(crate) branch: BranchName,
    pub(crate) epoch: nervix_models::PartitionEpochNumber,
    /// The count the epoch's previous count placed keys by.
    pub(crate) previous: nervix_models::BranchPartitions,
    /// The units beyond the current count's this node stays scheduled as for the keys the
    /// previous count placed there.
    pub(crate) holding: Vec<Unit>,
}

impl NodePartitionTransition {
    fn of(node: &ScheduledNode, schedule: &DomainSchedule) -> Option<Self> {
        if !node.keeps_state_per_key() {
            return None;
        }
        let branch = node.partition_branch()?;
        let epoch = schedule.partition_epochs.get(branch)?;
        let transition = epoch.transition.as_ref()?;
        Some(Self {
            branch: branch.clone(),
            epoch: epoch.number,
            previous: transition.previous,
            holding: epoch.holding_units(),
        })
    }

    /// Whether `unit` holds only keys the previous count placed there: no producer places a key
    /// in it under the current count.
    pub(crate) fn is_holding(&self, unit: Unit) -> bool {
        self.holding.contains(&unit)
    }
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
            units: node
                .units
                .iter()
                .map(|(unit, scheduled)| {
                    (
                        *unit,
                        ExecutionUnit {
                            primary_node: scheduled.primary_node.clone(),
                            assigned_nodes: scheduled.assigned_nodes.clone(),
                            ownership_transition: scheduled.ownership_transition.clone(),
                            wasm_state_generations: scheduled.wasm_state_generations.clone(),
                            wasm_state_reset: scheduled.wasm_state_reset.clone(),
                        },
                    )
                })
                .collect(),
            materialized_relay,
            ownership_state_components: node.ownership_state_components(),
            gate_relays: entity_pause_relays_for_schedule(
                schedule,
                std::slice::from_ref(&node.identity()),
            ),
            executes_on_every_cluster_node: node.config.executes_on_every_cluster_node(),
            partition_transition: NodePartitionTransition::of(node, schedule),
        }
    }

    /// Whether this node is scheduled as `unit` only for the keys its branch's previous partition
    /// count placed there.
    pub(crate) fn is_holding_unit(&self, unit: Unit) -> bool {
        match &self.partition_transition {
            Some(transition) => transition.is_holding(unit),
            None => false,
        }
    }

    pub(crate) fn kind(&self) -> ModelKind {
        self.identity.kind
    }

    pub(crate) fn identity(&self) -> NodeRef {
        self.identity.clone()
    }

    /// The cluster node `unit` executes on, or `None` for a node that runs on every cluster node
    /// and for a unit this node is not revised as.
    pub(crate) fn unit_execution_node(&self, unit: Unit) -> Option<&ClusterNodeName> {
        if self.executes_on_every_cluster_node {
            return None;
        }
        self.units.get(&unit)?.execution_node()
    }

    /// Whether `unit` executes on `node`.
    pub(crate) fn unit_executes_on(&self, unit: Unit, node: &ClusterNodeName) -> bool {
        let Some(scheduled) = self.units.get(&unit) else {
            return false;
        };
        if self.executes_on_every_cluster_node {
            scheduled.is_assigned_to(node)
        } else {
            scheduled.is_primary_on(node)
        }
    }

    /// The units that execute on `node`, in unit order.
    pub(crate) fn units_executing_on(&self, node: &ClusterNodeName) -> BTreeSet<Unit> {
        self.units
            .keys()
            .copied()
            .filter(|unit| self.unit_executes_on(*unit, node))
            .collect()
    }

    /// Where each unit's owner executes, for every unit a cluster node owns.
    pub(crate) fn unit_execution_nodes(&self) -> BTreeMap<Unit, ClusterNodeName> {
        let mut owners = BTreeMap::new();
        for unit in self.units.keys() {
            if let Some(owner) = self.unit_execution_node(*unit) {
                owners.insert(*unit, owner.clone());
            }
        }
        owners
    }

    pub(crate) fn ownership_state_components(&self) -> Vec<OwnershipStateComponent> {
        self.ownership_state_components.clone()
    }

    /// Whether any unit of this node is assigned to `node`, as its primary or a replica.
    pub(crate) fn is_assigned_to(&self, node: &ClusterNodeName) -> bool {
        self.units.values().any(|unit| unit.is_assigned_to(node))
    }

    /// The one unit of a node revised as a single unit, and `None` for a relay revised as several
    /// partitions, whose units each have their own placement.
    pub(crate) fn sole_unit(&self) -> Option<&ExecutionUnit> {
        self.units.get(&Unit::SINGLE)
    }

    /// The one node a single-unit node is assigned to, when it is assigned to exactly one. `None`
    /// for a node revised as several units.
    pub(crate) fn assigned_single_node(&self) -> Option<&ClusterNodeName> {
        match self.sole_unit()?.assigned_nodes.as_slice() {
            [node] => Some(node),
            _ => None,
        }
    }

    /// The primary of a single-unit node. `None` for a node revised as several units.
    pub(crate) fn primary_node(&self) -> Option<&ClusterNodeName> {
        self.sole_unit()?.primary_node.as_ref()
    }

    /// Whether any unit of this node is primary on `node`.
    pub(crate) fn is_primary_on(&self, node: &ClusterNodeName) -> bool {
        self.units.values().any(|unit| unit.is_primary_on(node))
    }

    /// The cluster node a single-unit node executes on. `None` for a node that runs on every
    /// cluster node, and for a node revised as several units, which have no one execution node.
    pub(crate) fn execution_node(&self) -> Option<&ClusterNodeName> {
        if self.executes_on_every_cluster_node {
            None
        } else {
            self.primary_node().or_else(|| self.assigned_single_node())
        }
    }

    /// Whether any unit of this node executes on `node`.
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
    /// The partition epoch of every branch whose count changed since it was created; a branch
    /// absent here is in its first epoch, with every key in its partition.
    pub(crate) partition_epochs: BTreeMap<BranchName, BranchPartitionEpoch>,
}

impl ExecutionRevision {
    #[cfg(test)]
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
        for node in schedule.nodes.values() {
            if !node.units_match_declaration(&schedule.holding_units_of(node)) {
                return Err(Report::new(ExecutionRevisionError::UnitDeclaration {
                    domain: domain.clone(),
                    node: node.identity(),
                }));
            }
        }
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
            partition_epochs: schedule.partition_epochs.clone(),
        }))
    }

    /// Fingerprint the committed placement and guest-state lifetime of every unit.
    /// Non-security BLAKE3: identifies a trusted schedule predecessor, not an ownership credential.
    pub(crate) fn ownership_fingerprint(
        schedule: &DomainSchedule,
    ) -> error_stack::Result<[u8; 32], ExecutionRevisionError> {
        #[derive(serde::Serialize)]
        struct ScheduledUnitFingerprint<'a> {
            unit: Unit,
            primary_node: &'a Option<ClusterNodeName>,
            assigned_nodes: &'a [ClusterNodeName],
            wasm_state_generations: Option<&'a WasmStateGenerations>,
            wasm_state_reset: Option<&'a WasmStateReset>,
        }

        #[derive(serde::Serialize)]
        struct ScheduledNodeFingerprint<'a> {
            identifier: &'a ModelName,
            config: &'a nervix_models::Model,
            resolved_branching: &'a Option<ResolvedBranching>,
            schema_fingerprint: SchemaFingerprint,
            kafka_partition_schedule: &'a Option<KafkaPartitionSchedule>,
            units: Vec<ScheduledUnitFingerprint<'a>>,
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
                units: node
                    .units
                    .iter()
                    .map(|(unit, scheduled)| ScheduledUnitFingerprint {
                        unit: *unit,
                        primary_node: &scheduled.primary_node,
                        assigned_nodes: &scheduled.assigned_nodes,
                        wasm_state_generations: scheduled.wasm_state_generations.as_ref(),
                        wasm_state_reset: scheduled.wasm_state_reset.as_ref(),
                    })
                    .collect(),
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

    /// Non-security BLAKE3: identifies the trusted schedule used to construct one execution revision.
    fn source_digest(
        schedule: &DomainSchedule,
    ) -> error_stack::Result<[u8; 32], ExecutionRevisionError> {
        // JSON object keys cannot encode NodeRef or numbered Unit values. Preserve the complete
        // ordered schedule as pairs at both map boundaries.
        #[derive(serde::Serialize)]
        struct NodeDigest<'a> {
            identity: &'a NodeRef,
            identifier: &'a ModelName,
            config: &'a nervix_models::Model,
            resolved_branching: &'a Option<ResolvedBranching>,
            schema_fingerprint: SchemaFingerprint,
            kafka_partition_schedule: &'a Option<KafkaPartitionSchedule>,
            units: Vec<(&'a Unit, &'a nervix_models::ScheduledUnit)>,
        }
        let nodes = schedule
            .nodes
            .iter()
            .map(|(identity, node)| NodeDigest {
                identity,
                identifier: &node.identifier,
                config: node.config.as_ref(),
                resolved_branching: &node.resolved_branching,
                schema_fingerprint: node.schema_fingerprint,
                kafka_partition_schedule: &node.kafka_partition_schedule,
                units: node.units.iter().collect(),
            })
            .collect::<Vec<_>>();
        // Partition epochs are part of the source: a retirement may end a transition without
        // changing any node's units.
        let epochs = schedule.partition_epochs.iter().collect::<Vec<_>>();
        let encoded =
            serde_json::to_vec(&(&schedule.domain, nodes, &schedule.placement_groups, epochs))
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
    pub(crate) reassignments: Vec<NodeUnitRef>,
    pub(crate) dynamic_updates: Vec<DynamicExecutionUpdate>,
    pub(crate) state_purges: BTreeMap<NodeRef, Vec<StatePurge>>,
    pub(crate) gate_relays: Vec<nervix_models::RelayName>,
    /// Input routes whose preceding delivery generation must end before the local swap fence.
    pub(crate) forced_recovery_relays: Vec<nervix_models::RelayName>,
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
    /// The partition transition of the branch dividing `node` retired: the node's units stop
    /// claiming keys in place, and the units that held only the previous count's keys end.
    PartitionTransitionRetired {
        node: NodeRef,
    },
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
            DynamicModelUpdate::PartitionTransitionRetired { node } => {
                Self::PartitionTransitionRetired { node }
            }
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
                let mut forced_recovery_relays = Vec::new();
                let gate_relays = if let Some(schedule) = desired {
                    let mut gated = entities.clone();
                    gated.extend(reassignments.iter().map(|unit| unit.node.clone()));
                    let mut forced = Vec::new();
                    for reassigned in &reassignments {
                        let Some(node) = schedule.nodes.get(&reassigned.node) else {
                            continue;
                        };
                        let Some(previous) = existing else {
                            continue;
                        };
                        let Some(before) = previous.nodes.get(&reassigned.node) else {
                            continue;
                        };
                        let (Some(before_unit), Some(after_unit)) = (
                            before.units.get(&reassigned.unit),
                            node.units.get(&reassigned.unit),
                        ) else {
                            continue;
                        };
                        // A completed transition remains in the schedule through later replica
                        // changes. Only a new execution-owner move retires delivery channels.
                        if before_unit.primary_node == after_unit.primary_node {
                            continue;
                        }
                        if let Some(OwnershipTransition {
                            state_recovery:
                                OwnershipStateRecoveryOutcome::Unverified
                                | OwnershipStateRecoveryOutcome::Reset,
                            ..
                        }) = &after_unit.ownership_transition
                        {
                            forced.push(reassigned.node.clone());
                        }
                    }
                    forced_recovery_relays = entity_pause_relays_for_schedule(schedule, &forced);
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
                    forced_recovery_relays,
                })
            }
            ScheduleDelta::Rebuild => Self::Rebuild,
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_arbitrary::{Arbitrary, Domain};
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
        node.single_unit_mut().primary_node = Some(owner.clone());
        node.single_unit_mut().assigned_nodes = vec![owner.clone()];

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
    fn bolero_execution_revision_preserves_unit_placement_and_reassignment() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(128)
            .for_each(|bytes: &[u8]| {
                let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
                let relay = NodeRef::new(ModelKind::Relay, named::<ModelName>("events"));
                let partition_count = u16::from(2 + arbitrary.entropy().byte() % 7);
                let partitions = nervix_models::BranchPartitions::Direct(
                    std::num::NonZeroU16::new(partition_count)
                        .assured("the property declares between two and eight partitions"),
                );
                let unit = Unit::partition(
                    u32::from(arbitrary.entropy().byte()) % u32::from(partition_count),
                );
                let owner = named::<ClusterNodeName>("node-1");
                let destination = named::<ClusterNodeName>("node-2");
                let mut before = schedule(nonzero!(2usize));
                let key_schema = before
                    .configured::<CreateSchema>(named::<ModelName>("event"))
                    .assured("the schedule declares its key schema")
                    .clone();
                let Model::Branch(mut branch) =
                    crate::registry::test_fixtures::branch("by_value", "event")
                else {
                    unreachable!("the branch fixture constructs a branch");
                };
                branch.partitions = partitions;
                let branching =
                    ResolvedBranching::branched(branch.name.clone(), key_schema, partitions);
                let branch = ScheduledNode::new(
                    Model::Branch(branch),
                    SchemaFingerprint::from_digest([7; 32]),
                );
                before.nodes.insert(branch.identity(), branch);
                let node = before
                    .nodes
                    .get_mut(&relay)
                    .assured("the fixture schedules its relay");
                let Model::Relay(mut config) = *node.config.clone() else {
                    unreachable!("the scheduled relay carries a relay model");
                };
                config.branching = RelayBranching::branched_by(named("by_value"));
                *node = ScheduledNode::new(Model::Relay(config), node.schema_fingerprint)
                    .with_resolved_branching(Some(branching))
                    .placed_on(Some(owner.clone()), vec![owner.clone()]);
                let partition = node
                    .units
                    .get_mut(&unit)
                    .assured("the selected partition belongs to its declaration");
                partition.primary_node = Some(destination.clone());
                partition.assigned_nodes = vec![destination.clone()];

                let revision = ExecutionRevision::from_schedule(&before)
                    .assured("the unit-keyed schedule plans");
                let planned = &revision.nodes[&relay];
                assert_eq!(planned.units.len(), usize::from(partition_count));
                for (key, placement) in &planned.units {
                    let expected = if *key == unit { &destination } else { &owner };
                    assert_eq!(placement.primary_node.as_ref(), Some(expected));
                    assert_eq!(placement.assigned_nodes, vec![expected.clone()]);
                }
                assert_eq!(
                    revision.ownership_handoff_fingerprint,
                    ExecutionRevision::ownership_fingerprint(&before)
                        .assured("the schedule fingerprints"),
                );

                let mut after = before.clone();
                let moved = after
                    .nodes
                    .get_mut(&relay)
                    .assured("the relay remains scheduled");
                moved
                    .units
                    .get_mut(&unit)
                    .assured("the partition remains scheduled")
                    .primary_node = Some(owner.clone());
                moved
                    .units
                    .get_mut(&unit)
                    .assured("the partition remains scheduled")
                    .assigned_nodes = vec![owner.clone()];
                assert!(matches!(
                    ScheduleDelta::between(Some(&before), Some(&after)),
                    ScheduleDelta::EntitySwap { reassignments, .. }
                        if reassignments == vec![NodeUnitRef::new(relay.clone(), unit)]
                ));
                for (key, assignment) in &before.nodes[&relay].units {
                    if *key != unit {
                        assert_eq!(after.nodes[&relay].units[key], *assignment);
                    }
                }
                assert_ne!(
                    revision.ownership_handoff_fingerprint,
                    ExecutionRevision::ownership_fingerprint(&after)
                        .assured("the moved unit fingerprints"),
                );
            });
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
        node.single_unit_mut().primary_node = Some(source.clone());
        node.single_unit_mut().assigned_nodes = vec![source];
        let mut after = before.clone();
        let node = after
            .nodes
            .get_mut(&relay)
            .assured("the fixture schedules its relay");
        node.single_unit_mut().primary_node = Some(destination.clone());
        node.single_unit_mut().assigned_nodes = vec![destination];

        assert!(
            matches!(ExecutionDelta::between(Some(&before), Some(&after)),
            ExecutionDelta::EntitySwap(change) if change.reassignments == vec![NodeUnitRef::new(relay, Unit::SINGLE)])
        );
    }

    #[rstest::rstest]
    #[case(None, false)]
    #[case(Some(OwnershipStateRecoveryOutcome::Complete), false)]
    #[case(Some(OwnershipStateRecoveryOutcome::Unverified), true)]
    #[case(Some(OwnershipStateRecoveryOutcome::Reset), true)]
    fn reassignment_plans_routing_retirement_for_forced_recovery(
        #[case] recovery: Option<OwnershipStateRecoveryOutcome>,
        #[case] forced: bool,
    ) {
        let mut before = schedule(nonzero!(2usize));
        let relay = NodeRef::new(ModelKind::Relay, named::<ModelName>("events"));
        let source = named::<ClusterNodeName>("node-1");
        let destination = named::<ClusterNodeName>("node-2");
        let node = before
            .nodes
            .get_mut(&relay)
            .assured("the fixture contains its relay");
        node.single_unit_mut().primary_node = Some(source.clone());
        node.single_unit_mut().assigned_nodes = vec![source.clone()];
        let mut after = before.clone();
        let node = after
            .nodes
            .get_mut(&relay)
            .assured("the target contains the same relay");
        node.single_unit_mut().primary_node = Some(destination.clone());
        node.single_unit_mut().assigned_nodes = vec![destination.clone()];
        if let Some(state_recovery) = recovery {
            node.single_unit_mut().ownership_transition = Some(OwnershipTransition {
                id: "forced-routing-test".into(),
                source,
                destination,
                state_recovery,
                resets: Vec::new(),
            });
        }
        let ExecutionDelta::EntitySwap(change) =
            ExecutionDelta::between(Some(&before), Some(&after))
        else {
            panic!("a reassignment is a typed entity swap");
        };
        let expected: Vec<nervix_models::RelayName> = if forced {
            vec![named("events")]
        } else {
            Vec::new()
        };
        assert_eq!(change.forced_recovery_relays, expected);
        assert_eq!(
            change.reassignments,
            vec![NodeUnitRef::new(relay, Unit::SINGLE)]
        );
        assert!(matches!(
            ExecutionDelta::between(Some(&after), Some(&after)),
            ExecutionDelta::Unchanged
        ));
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

    #[test]
    fn replica_assignment_change_uses_ordinary_quiescence() {
        let mut before = schedule(nonzero!(2usize));
        let relay = NodeRef::new(ModelKind::Relay, named::<ModelName>("events"));
        let owner = named::<ClusterNodeName>("node-1");
        let node = before
            .nodes
            .get_mut(&relay)
            .assured("the fixture contains its relay");
        node.single_unit_mut().primary_node = Some(owner.clone());
        node.single_unit_mut().assigned_nodes = vec![owner.clone()];
        node.single_unit_mut().ownership_transition = Some(OwnershipTransition {
            id: "retained-routing-transition".into(),
            source: named("node-3"),
            destination: owner,
            state_recovery: OwnershipStateRecoveryOutcome::Unverified,
            resets: Vec::new(),
        });
        let mut after = before.clone();
        after
            .nodes
            .get_mut(&relay)
            .assured("the target contains the relay")
            .single_unit_mut()
            .assigned_nodes
            .push(named("node-2"));
        let ExecutionDelta::EntitySwap(change) =
            ExecutionDelta::between(Some(&before), Some(&after))
        else {
            panic!("a replica assignment change has a typed swap plan");
        };
        assert_eq!(
            change.forced_recovery_relays,
            Vec::<nervix_models::RelayName>::new()
        );
        assert_eq!(
            change.reassignments,
            vec![NodeUnitRef::new(relay, Unit::SINGLE)]
        );
    }
}
