//! The schedule a cluster publishes for every domain's nodes.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The cluster and domain schedules, the placement and state lifetime of each unit of
//!   a scheduled node, Kafka partition assignments, and recorded ownership transitions and resets.
//! - **Depends on.** The Models a scheduled node configures, node and cluster names, and the WASM
//!   state generations a transition carries.
//! - **Must not know.** How a placement is decided, how a schedule is applied to a node, or how
//!   state moves between owners.
//!
//! This is replicated control-plane state, which belongs to consensus rather than to the
//! vocabulary: the crate root names that contradiction.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{collections::BTreeMap, num::NonZeroU64};

use indexmap::IndexMap;
use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use crate::{
    BranchKeyFingerprint, ClusterNodeName, CommandExecutionReference, CreateWasmProcessor,
    DomainName, DomainNodeRef, IngestorEntity, KafkaIngestor, KafkaOffsetMode, Model, ModelKind,
    ModelName, NodeRef, ResolvedBranching, SchemaFingerprint, UniquelyKindedModel,
    WasmSavedStateRejection, WasmStateGeneration, WasmStateGenerations, WasmStateRecoveries,
    WasmStateRecoveryAdmission, WasmStateRecoveryOutcome, WasmStateReset, WasmStateResetPhase,
    WasmStateResetReason, WasmStateResetScope,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClusterSchedule {
    pub domains: BTreeMap<DomainName, DomainSchedule>,
}

impl ClusterSchedule {
    pub fn domain(&self, domain: &DomainName) -> Option<&DomainSchedule> {
        self.domains.get(domain)
    }
}

impl FromIterator<DomainSchedule> for ClusterSchedule {
    fn from_iter<I: IntoIterator<Item = DomainSchedule>>(schedules: I) -> Self {
        Self {
            domains: schedules
                .into_iter()
                .map(|schedule| (schedule.domain.clone(), schedule))
                .collect(),
        }
    }
}

/// A domain's scheduled nodes, in the order the registry emitted them and keyed by runtime node
/// identity so callers resolve a node by kind and identifier without scanning the sequence.
pub type ScheduledNodes = IndexMap<NodeRef, ScheduledNode>;

/// One independently placed execution of a logical node. Partition and instance numbers are
/// zero-based within that node. A graph without declared partitions or external instances has
/// exactly one `Single` unit.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum Unit {
    Single,
    Partition(u32),
    Instance(u32),
}

/// The identity of one unit of a scheduled node, shared by planning and state ownership.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct NodeUnitRef {
    pub node: NodeRef,
    pub unit: Unit,
}

impl NodeUnitRef {
    pub fn new(node: NodeRef, unit: Unit) -> Self {
        Self { node, unit }
    }
}

/// A scheduled unit in its owning domain. Shared by state assignment and ownership records.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DomainNodeUnitRef {
    pub entity: DomainNodeRef,
    pub unit: Unit,
}

impl DomainNodeUnitRef {
    pub fn new(entity: DomainNodeRef, unit: Unit) -> Self {
        Self { entity, unit }
    }

    pub fn single(entity: DomainNodeRef) -> Self {
        Self::new(entity, Unit::Single)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainSchedule {
    pub domain: DomainName,
    pub nodes: ScheduledNodes,
    pub placement_groups: Vec<PlacementGroupSchedule>,
}

impl DomainSchedule {
    /// Builds a schedule from the registry's emitted node sequence, keeping that order and keying
    /// each node by its runtime identity.
    pub fn new(
        domain: DomainName,
        nodes: impl IntoIterator<Item = ScheduledNode>,
        placement_groups: Vec<PlacementGroupSchedule>,
    ) -> Self {
        Self {
            domain,
            nodes: nodes
                .into_iter()
                .map(|node| (node.identity(), node))
                .collect(),
            placement_groups,
        }
    }

    /// The `M` named `identifier` this schedule places, when it places one.
    ///
    /// Every entry is keyed by the kind of the configuration it carries, so an entry found under
    /// `M`'s kind carries an `M` and a reader has only the missing node to answer for.
    pub fn configured<M: UniquelyKindedModel>(
        &self,
        identifier: impl Into<ModelName>,
    ) -> Option<&M> {
        self.scheduled(identifier).map(|scheduled| scheduled.config)
    }

    /// The `M` named `identifier` together with the entry that places it.
    ///
    /// Readers that need both the configuration and where it runs take them from one lookup, so
    /// the two can never come from different entries.
    pub fn scheduled<M: UniquelyKindedModel>(
        &self,
        identifier: impl Into<ModelName>,
    ) -> Option<ScheduledModel<'_, M>> {
        let node = self.nodes.get(&NodeRef::new(M::KIND, identifier.into()))?;
        Some(ScheduledModel {
            config: M::from_model_ref(&node.config)
                .assured("a schedule keys every entry by the kind of the configuration it carries"),
            node,
        })
    }

    /// The units whose state this cluster node is responsible for archiving.
    pub fn primary_units_on(&self, node: &ClusterNodeName) -> Vec<NodeUnitRef> {
        self.nodes
            .iter()
            .flat_map(|(entity, scheduled)| {
                scheduled.units.iter().filter_map(move |(unit, placement)| {
                    (placement.primary_node.as_ref() == Some(node))
                        .then(|| NodeUnitRef::new(entity.clone(), *unit))
                })
            })
            .collect()
    }
}

/// One scheduled node with its configuration already narrowed to the shape its kind names.
#[derive(Debug, Clone, Copy)]
pub struct ScheduledModel<'a, M> {
    pub config: &'a M,
    pub node: &'a ScheduledNode,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PlacementGroupSchedule {
    pub members: Vec<NodeRef>,
    pub unit: Unit,
    pub primary_node: Option<ClusterNodeName>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct KafkaPartitionSchedule {
    pub observed_partitions: Vec<i32>,
    pub rebalance_epoch: u64,
    pub instance_assignments: Vec<Vec<i32>>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Serialize,
    Deserialize,
    AsRefStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum OwnershipStateComponent {
    BranchAggregated,
    BranchLifecycle,
    Deduplicator,
    KafkaOffsets,
    MaterializedRelay,
    WasmProcessor,
    WindowProcessor,
}

impl OwnershipStateComponent {
    /// Whether a backup archive carries this component's state. Branch-aggregated metrics are
    /// never archived: a restored entity starts its metrics afresh.
    pub fn is_archived(self) -> bool {
        match self {
            Self::BranchAggregated => false,
            Self::BranchLifecycle
            | Self::Deduplicator
            | Self::KafkaOffsets
            | Self::MaterializedRelay
            | Self::WasmProcessor
            | Self::WindowProcessor => true,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Serialize,
    Deserialize,
    AsRefStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum OwnershipStateRecoveryOutcome {
    Complete,
    Unverified,
    Reset,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Serialize,
    Deserialize,
    AsRefStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum OwnershipStateResetCause {
    MissingCheckpoint,
    InvalidCheckpoint,
    ConflictingCheckpoint,
    ExpiredBranchMetadata,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize,
)]
pub struct OwnershipStateReset {
    pub component: OwnershipStateComponent,
    pub cause: OwnershipStateResetCause,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OwnershipTransition {
    pub id: String,
    pub source: ClusterNodeName,
    pub destination: ClusterNodeName,
    pub state_recovery: OwnershipStateRecoveryOutcome,
    pub resets: Vec<OwnershipStateReset>,
}

impl KafkaPartitionSchedule {
    pub fn new(instances: NonZeroU64, observed_partitions: Vec<i32>, rebalance_epoch: u64) -> Self {
        let shard_count = usize::try_from(instances.get()).unwrap_or(usize::MAX);
        let mut observed_partitions = observed_partitions;
        observed_partitions.sort_unstable();
        let mut instance_assignments = vec![Vec::new(); shard_count];
        for (ordinal, partition) in observed_partitions.iter().copied().enumerate() {
            let instance_idx = ordinal % shard_count;
            if let Some(assigned) = instance_assignments.get_mut(instance_idx) {
                assigned.push(partition);
            }
        }
        Self {
            observed_partitions,
            rebalance_epoch,
            instance_assignments,
        }
    }
}

/// Placement, ownership transition and guest-state lifetime of one execution unit.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ScheduledUnit {
    pub primary_node: Option<ClusterNodeName>,
    pub assigned_nodes: Vec<ClusterNodeName>,
    pub ownership_transition: Option<OwnershipTransition>,
    /// Present only for a WASM processor. Every unit owns its own branch generations.
    pub wasm_state_generations: Option<WasmStateGenerations>,
    pub wasm_state_reset: Option<WasmStateReset>,
    pub wasm_state_recoveries: Option<WasmStateRecoveries>,
}

impl ScheduledUnit {
    pub fn is_primary_on(&self, node: &ClusterNodeName) -> bool {
        self.primary_node
            .as_ref()
            .is_some_and(|primary| primary == node)
            || (self.primary_node.is_none() && self.assigned_nodes.contains(node))
    }

    pub fn execution_node(&self) -> Option<&ClusterNodeName> {
        self.primary_node
            .as_ref()
            .or_else(|| match self.assigned_nodes.as_slice() {
                [node] => Some(node),
                _ => None,
            })
    }

    pub fn replica_nodes(&self) -> Vec<&ClusterNodeName> {
        self.assigned_nodes
            .iter()
            .filter(|node| Some(*node) != self.primary_node.as_ref())
            .collect()
    }

    fn for_model(config: &Model) -> Self {
        let (wasm_state_generations, wasm_state_recoveries) = match config {
            Model::WasmProcessor(_) => (
                Some(WasmStateGenerations::first()),
                Some(WasmStateRecoveries::empty()),
            ),
            _ => (None, None),
        };
        Self {
            primary_node: None,
            assigned_nodes: Vec::new(),
            ownership_transition: None,
            wasm_state_generations,
            wasm_state_reset: None,
            wasm_state_recoveries,
        }
    }
}

/// One logical runtime node of a domain schedule: its configuration and execution units.
///
/// The kind and the identifier are read from the configuration when the entry is built, so an
/// entry can never describe itself as a node its configuration is not. That is why every reader
/// resolving an entry by kind receives the shape that kind names, with nothing left to reject.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[non_exhaustive]
pub struct ScheduledNode {
    pub identifier: ModelName,
    pub config: Box<Model>,
    pub resolved_branching: Option<ResolvedBranching>,
    /// The fingerprint of the schemas this node's records are laid out by, as the registry computed
    /// it. Every scheduled node carries one; the runtime keys each of the node's schema-bound states
    /// by it.
    pub schema_fingerprint: SchemaFingerprint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_partition_schedule: Option<KafkaPartitionSchedule>,
    pub units: IndexMap<Unit, ScheduledUnit>,
}

impl ScheduledNode {
    /// An unplaced entry for `config`, whose records are laid out by the schemas `schema_fingerprint`
    /// covers, before a scheduler decides which cluster nodes run it.
    pub fn new(config: Model, schema_fingerprint: SchemaFingerprint) -> Self {
        let unit = ScheduledUnit::for_model(&config);
        Self {
            identifier: config.name(),
            config: Box::new(config),
            resolved_branching: None,
            schema_fingerprint,
            kafka_partition_schedule: None,
            units: IndexMap::from([(Unit::Single, unit)]),
        }
    }

    /// The complete branch declaration the registry resolved for this node.
    #[must_use]
    pub fn with_resolved_branching(mut self, branching: Option<ResolvedBranching>) -> Self {
        self.resolved_branching = branching;
        self
    }

    /// The Kafka partitions the leader observed for this node, and how they are shared out.
    #[must_use]
    pub fn with_kafka_partitions(mut self, partitions: KafkaPartitionSchedule) -> Self {
        self.kafka_partition_schedule = Some(partitions);
        self
    }

    /// The cluster nodes this entry is placed on, with `primary` owning its state.
    #[must_use]
    pub fn placed_on(
        mut self,
        primary: Option<ClusterNodeName>,
        assigned: Vec<ClusterNodeName>,
    ) -> Self {
        let unit = self.single_unit_mut();
        unit.primary_node = primary;
        unit.assigned_nodes = assigned;
        self
    }

    /// The sole unit of a graph that has not declared partitions or external instances.
    pub fn single_unit(&self) -> &ScheduledUnit {
        self.units
            .get(&Unit::Single)
            .assured("current graphs construct exactly one Single unit per node")
    }

    /// The sole unit of a graph that has not declared partitions or external instances.
    pub fn single_unit_mut(&mut self) -> &mut ScheduledUnit {
        self.units
            .get_mut(&Unit::Single)
            .assured("current graphs construct exactly one Single unit per node")
    }

    /// The kind of runtime node this entry configures.
    pub fn kind(&self) -> ModelKind {
        self.config.kind()
    }

    pub fn wasm_processor(&self) -> Option<&CreateWasmProcessor> {
        let Model::WasmProcessor(processor) = self.config.as_ref() else {
            return None;
        };
        Some(processor)
    }

    /// The guest-state generation of every branch, when this node is a WASM processor.
    pub fn wasm_state_generations(&self) -> Option<&WasmStateGenerations> {
        self.single_unit().wasm_state_generations.as_ref()
    }

    /// The latest coordinated guest-state reset this WASM processor published.
    pub fn wasm_state_reset(&self) -> Option<&WasmStateReset> {
        self.single_unit().wasm_state_reset.as_ref()
    }

    /// The recovery attempts this WASM processor's refused guest-state lifetimes have spent.
    pub fn wasm_state_recoveries(&self) -> Option<&WasmStateRecoveries> {
        self.single_unit().wasm_state_recoveries.as_ref()
    }

    /// Decide what the refused `generation` of `scope` is entitled to, recording an admitted
    /// attempt so that a restart, a leader change, or another owner reads the same decision.
    ///
    /// Returns `None` when this node holds no WASM guest state, and therefore nothing that could
    /// have been refused.
    pub fn admit_wasm_state_recovery(
        &mut self,
        scope: WasmStateResetScope,
        generation: WasmStateGeneration,
        rejection: WasmSavedStateRejection,
    ) -> Option<WasmStateRecoveryAdmission> {
        let recoveries = self.single_unit_mut().wasm_state_recoveries.as_mut()?;
        let request = scope.recovery_request(generation);
        Some(recoveries.admit(scope, generation, rejection, request))
    }

    /// Record what the admitted recovery attempt of `scope` achieved.
    ///
    /// Returns `false` when this node holds no unresolved attempt for `scope` under `request`.
    pub fn settle_wasm_state_recovery(
        &mut self,
        scope: &WasmStateResetScope,
        request: &CommandExecutionReference,
        outcome: WasmStateRecoveryOutcome,
    ) -> bool {
        let Some(recoveries) = self.single_unit_mut().wasm_state_recoveries.as_mut() else {
            return false;
        };
        recoveries.settle(scope, request, outcome)
    }

    /// Continue the guest-state lifetimes `existing` published for this same node.
    ///
    /// A generation belongs to the pinned module binding it was published for. When this entry
    /// binds a different resource version or file than `existing`, the lifetimes continue from
    /// `existing` and every branch then starts a new one, so no snapshot saved under the previous
    /// binding can attach to this one.
    pub fn continue_wasm_state_generations_of(&mut self, existing: &Self) {
        let (Some(processor), Some(existing_processor)) =
            (self.wasm_processor(), existing.wasm_processor())
        else {
            return;
        };
        let binding_changed = processor.resource != existing_processor.resource
            || processor.resource_version != existing_processor.resource_version
            || processor.file != existing_processor.file;
        let existing_unit = existing.single_unit();
        let Some(existing_generations) = existing_unit.wasm_state_generations.as_ref() else {
            return;
        };
        let mut generations = existing_generations.clone();
        if binding_changed {
            generations.begin_every_branch();
        }
        let unit = self.single_unit_mut();
        unit.wasm_state_generations = Some(generations);
        unit.wasm_state_reset = if binding_changed {
            None
        } else {
            existing_unit.wasm_state_reset.clone()
        };
        // A recovery attempt is spent on the lifetime it was admitted for. A changed binding
        // restarts every lifetime, so the attempts recorded against the previous ones no longer
        // name anything this entry can refuse.
        unit.wasm_state_recoveries = match binding_changed {
            true => Some(WasmStateRecoveries::empty()),
            false => existing_unit.wasm_state_recoveries.clone(),
        };
    }

    /// Start a new guest-state lifetime for every branch of this WASM processor, including branches
    /// that exist only as persisted snapshots. Every other kind of node has no guest state.
    pub fn begin_wasm_state_generation(&mut self) {
        if let Some(generations) = self.single_unit_mut().wasm_state_generations.as_mut() {
            generations.begin_every_branch();
        }
    }

    /// Start a new guest-state lifetime for one concrete branch of this WASM processor, leaving every
    /// other branch in the lifetime it has.
    pub fn begin_wasm_branch_state_generation(&mut self, branch: BranchKeyFingerprint) {
        if let Some(generations) = self.single_unit_mut().wasm_state_generations.as_mut() {
            generations.begin_branch(branch);
        }
    }

    /// Start or resume the coordinated reset identified by `request`.
    ///
    /// Returns `false` when this node has no WASM guest state or when this exact request and scope
    /// are already published. A caller validates a reused request with a different scope before
    /// invoking this method.
    pub fn begin_wasm_state_reset(
        &mut self,
        request: CommandExecutionReference,
        scope: WasmStateResetScope,
        reason: WasmStateResetReason,
    ) -> bool {
        let unit = self.single_unit_mut();
        let Some(generations) = unit.wasm_state_generations.as_mut() else {
            return false;
        };
        if unit
            .wasm_state_reset
            .as_ref()
            .is_some_and(|reset| reset.request() == &request && reset.scope() == &scope)
        {
            return false;
        }
        generations.begin_reset(&scope);
        unit.wasm_state_reset = Some(WasmStateReset::publishing(request, scope, reason));
        true
    }

    /// Mark the initial checkpoint of this exact reset durable and ready for runtime admission.
    pub fn complete_wasm_state_reset(&mut self, request: &CommandExecutionReference) -> bool {
        let Some(reset) = self.single_unit_mut().wasm_state_reset.as_mut() else {
            return false;
        };
        if reset.request() != request || reset.phase() == WasmStateResetPhase::Ready {
            return false;
        }
        reset.mark_ready();
        true
    }

    pub fn ownership_state_components(&self) -> Vec<OwnershipStateComponent> {
        let mut components = Vec::new();
        if self.kind() != ModelKind::Relay {
            components.push(OwnershipStateComponent::BranchAggregated);
        }
        if let Model::Relay(relay) = self.config.as_ref()
            && relay.materialized_state.is_some()
        {
            components.push(OwnershipStateComponent::MaterializedRelay);
        }
        if let Model::Ingestor(ingestor) = self.config.as_ref()
            && let crate::IngestorInput::Transport(input) = &ingestor.input
            && let IngestorEntity::Kafka(KafkaIngestor::Topic(topic)) = &input.source
            && topic.offset_mode == KafkaOffsetMode::Domain
        {
            components.push(OwnershipStateComponent::KafkaOffsets);
        }
        if self.kind().is_processor()
            || matches!(self.kind(), ModelKind::Ingestor | ModelKind::Reingestor)
        {
            components.push(OwnershipStateComponent::BranchLifecycle);
        }
        match self.kind() {
            ModelKind::Deduplicator => {
                components.push(OwnershipStateComponent::Deduplicator);
            }
            ModelKind::WasmProcessor => {
                components.push(OwnershipStateComponent::WasmProcessor);
            }
            ModelKind::WindowProcessor => {
                components.push(OwnershipStateComponent::WindowProcessor);
            }
            _ => {}
        }
        components.sort();
        components
    }

    /// The runtime node this scheduled entry configures. Kind and identifier together name a node
    /// in a domain, and this is the key its schedule is stored under.
    pub fn identity(&self) -> NodeRef {
        NodeRef::new(self.kind(), self.identifier.clone())
    }

    fn executes_on_every_cluster_node(&self) -> bool {
        self.config.executes_on_every_cluster_node()
    }

    pub fn is_assigned_to(&self, node_id: &ClusterNodeName) -> bool {
        self.single_unit()
            .assigned_nodes
            .iter()
            .any(|assigned| assigned == node_id)
    }

    pub fn assigned_single_node(&self) -> Option<&ClusterNodeName> {
        match self.single_unit().assigned_nodes.as_slice() {
            [node_id] => Some(node_id),
            _ => None,
        }
    }

    pub fn primary_node(&self) -> Option<&ClusterNodeName> {
        self.single_unit().primary_node.as_ref()
    }

    pub fn replica_nodes(&self) -> Vec<&ClusterNodeName> {
        let primary = self.primary_node();
        self.single_unit()
            .assigned_nodes
            .iter()
            .filter(|node_id| Some(*node_id) != primary)
            .collect()
    }

    pub fn is_primary_on(&self, node_id: &ClusterNodeName) -> bool {
        if let Some(primary_node) = self.primary_node() {
            primary_node == node_id
        } else {
            self.is_assigned_to(node_id)
        }
    }

    pub fn execution_node(&self) -> Option<&ClusterNodeName> {
        if self.executes_on_every_cluster_node() {
            None
        } else {
            self.primary_node().or_else(|| self.assigned_single_node())
        }
    }

    pub fn executes_on(&self, node_id: &ClusterNodeName) -> bool {
        if self.executes_on_every_cluster_node() {
            self.is_assigned_to(node_id)
        } else {
            self.is_primary_on(node_id)
        }
    }

    /// Every node [`Self::executes_on`] accepts: each assigned node of an entity that runs on every
    /// cluster node, and otherwise the primary, or each assigned node when there is no primary.
    pub fn execution_nodes(&self) -> &[ClusterNodeName] {
        if self.executes_on_every_cluster_node() {
            return &self.single_unit().assigned_nodes;
        }
        match &self.single_unit().primary_node {
            Some(primary) => std::slice::from_ref(primary),
            None => &self.single_unit().assigned_nodes,
        }
    }

    /// True when `other` gives this runtime node the same primary owner and the same replica set.
    /// A schedule difference that fails this check is an assignment change, which activates
    /// narrowly instead of rebuilding the domain.
    pub fn has_same_assignment_as(&self, other: &Self) -> bool {
        self.units
            .iter()
            .map(|(key, unit)| (key, &unit.primary_node, &unit.assigned_nodes))
            .eq(other
                .units
                .iter()
                .map(|(key, unit)| (key, &unit.primary_node, &unit.assigned_nodes)))
    }
}

#[cfg(test)]
#[path = "schedule_tests.rs"]
mod tests;
