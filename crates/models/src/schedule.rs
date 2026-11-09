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

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

use indexmap::IndexMap;
use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use crate::{
    BranchKeyFingerprint, BranchName, BranchPartitionEpoch, ClusterNodeName,
    CommandExecutionReference, CreateWasmProcessor, DomainName, DomainNodeRef, IngestorEntity,
    KafkaIngestor, KafkaOffsetMode, Model, ModelKind, ModelName, NodeRef, PartitionEpochNumber,
    PartitionTransition, ResolvedBranching, SchemaFingerprint, UniquelyKindedModel, Unit,
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

/// The scope of an intake gate: a model operation selects complete logical entities, whereas an
/// ownership handoff selects only the units being transferred.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum EntityGateTargets {
    Entities(Vec<NodeRef>),
    Units(Vec<NodeUnitRef>),
}

impl Default for EntityGateTargets {
    fn default() -> Self {
        Self::Entities(Vec::new())
    }
}

impl EntityGateTargets {
    pub fn entities(&self) -> Vec<NodeRef> {
        match self {
            Self::Entities(entities) => entities.clone(),
            Self::Units(units) => units
                .iter()
                .map(|unit| unit.node.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
        }
    }

    pub fn contains(&self, member: &NodeUnitRef) -> bool {
        match self {
            Self::Entities(entities) => entities.contains(&member.node),
            Self::Units(units) => units.contains(member),
        }
    }

    pub fn normalized(&self) -> Self {
        match self {
            Self::Entities(entities) => Self::Entities(
                entities
                    .iter()
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            ),
            Self::Units(units) => Self::Units(
                units
                    .iter()
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            ),
        }
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
        Self::new(entity, Unit::SINGLE)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainSchedule {
    pub domain: DomainName,
    pub nodes: ScheduledNodes,
    pub placement_groups: Vec<PlacementGroupSchedule>,
    /// The partition epoch of every branch whose partition count changed since it was created. A
    /// branch without an entry keeps the count it was created with, in epoch one.
    pub partition_epochs: BTreeMap<BranchName, BranchPartitionEpoch>,
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
            partition_epochs: BTreeMap::new(),
        }
    }

    /// This schedule with `epochs` as the partition epochs of its branches.
    #[must_use]
    pub fn with_partition_epochs(
        mut self,
        epochs: BTreeMap<BranchName, BranchPartitionEpoch>,
    ) -> Self {
        self.partition_epochs = epochs;
        self
    }

    /// The partition epoch `branch` executes in: one until its count first changes.
    pub fn partition_epoch_of(&self, branch: &BranchName) -> PartitionEpochNumber {
        match self.partition_epochs.get(branch) {
            Some(epoch) => epoch.number,
            None => PartitionEpochNumber::FIRST,
        }
    }

    /// The transition `branch` is in while keys placed under its previous count may still lie
    /// outside their partition.
    pub fn partition_transition_of(&self, branch: &BranchName) -> Option<&PartitionTransition> {
        self.partition_epochs.get(branch)?.transition.as_ref()
    }

    /// This schedule once `branch`'s partition transition retired: no unit holds a key outside its
    /// partition any more, so the epoch keeps its count without a transition and every node that
    /// keeps state per key for the branch loses the units it kept for the previous count.
    #[must_use]
    pub fn with_partition_transition_retired(&self, branch: &BranchName) -> Self {
        let mut retired = self.clone();
        let Some(epoch) = retired.partition_epochs.get_mut(branch) else {
            return retired;
        };
        let holding = epoch.holding_units();
        *epoch = epoch.retired();
        for node in retired.nodes.values_mut() {
            if node.partition_branch() != Some(branch) || !node.keeps_state_per_key() {
                continue;
            }
            for unit in &holding {
                node.units.shift_remove(unit);
            }
        }
        retired
    }

    /// The units `node` keeps scheduled beyond its branch's current count, because they may still
    /// hold keys its branch's previous count placed there: none outside a transition.
    pub fn holding_units_of(&self, node: &ScheduledNode) -> Vec<Unit> {
        if !node.keeps_state_per_key() {
            return Vec::new();
        }
        let Some(branch) = node.partition_branch() else {
            return Vec::new();
        };
        match self.partition_epochs.get(branch) {
            Some(epoch) => epoch.holding_units(),
            None => Vec::new(),
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
                scheduled
                    .units
                    .iter()
                    .filter(move |(_, placement)| placement.primary_node.as_ref() == Some(node))
                    .map(move |(unit, _)| NodeUnitRef::new(entity.clone(), *unit))
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
    pub members: Vec<NodeUnitRef>,
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
    Inferencer,
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
            | Self::WindowProcessor
            | Self::Inferencer => true,
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
            .or(match self.assigned_nodes.as_slice() {
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

    pub fn is_assigned_to(&self, node: &ClusterNodeName) -> bool {
        self.assigned_nodes.iter().any(|assigned| assigned == node)
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
        let units = Self::units_for(&config, None)
            .into_iter()
            .map(|unit| (unit, ScheduledUnit::for_model(&config)))
            .collect();
        Self {
            identifier: config.name(),
            config: Box::new(config),
            resolved_branching: None,
            schema_fingerprint,
            kafka_partition_schedule: None,
            units,
        }
    }

    /// The complete branch declaration the registry resolved for this node, and the units it
    /// decides: a relay of a branch with several partitions is scheduled as one `Partition` unit
    /// per partition. Fixed-count ingestors declare `Instance` units; the registry supplies a
    /// per-partition ingestor's resolved units with [`Self::with_units`]. Units are unplaced until
    /// [`Self::placed_on`] or [`Self::place_unit`] places them.
    #[must_use]
    pub fn with_resolved_branching(mut self, branching: Option<ResolvedBranching>) -> Self {
        let units = Self::units_for(&self.config, branching.as_ref());
        if !units.iter().eq(self.units.keys()) {
            let config = self.config.as_ref();
            self.units = units
                .into_iter()
                .map(|unit| (unit, ScheduledUnit::for_model(config)))
                .collect();
        }
        self.resolved_branching = branching;
        self
    }

    /// Installs an explicit unit declaration. Units retained by the declaration retain their
    /// placements; newly declared units are unplaced.
    #[must_use]
    pub fn with_units(mut self, units: &[Unit]) -> Self {
        let mut previous = std::mem::take(&mut self.units);
        self.units = units
            .iter()
            .map(|unit| {
                let scheduled = previous
                    .swap_remove(unit)
                    .unwrap_or_else(|| ScheduledUnit::for_model(self.config.as_ref()));
                (*unit, scheduled)
            })
            .collect();
        self
    }

    /// The units a node configured by `config` is scheduled as under `branching`.
    fn units_for(config: &Model, branching: Option<&ResolvedBranching>) -> Vec<Unit> {
        if let Some(partitions) = Self::branch_partitions_for(config, branching) {
            return partitions.units();
        }
        match config {
            Model::Ingestor(ingestor) => match ingestor.input.instances() {
                Some(crate::IngestorInstances::Count(count)) => (0..count.get())
                    .map(|index| {
                        Unit::try_instance(index)
                            .assured("the validated allocation layout bounds the instance index")
                    })
                    .collect(),
                _ => vec![Unit::SINGLE],
            },
            _ => vec![Unit::SINGLE],
        }
    }

    fn branch_partitions_for(
        config: &Model,
        branching: Option<&ResolvedBranching>,
    ) -> Option<crate::BranchPartitions> {
        match config {
            Model::Relay(_)
            | Model::Junction(_)
            | Model::Deduplicator(_)
            | Model::Reorderer(_)
            | Model::WindowProcessor(_)
            | Model::Correlator(_)
            | Model::Inferencer(_)
            | Model::WasmProcessor(_)
            | Model::Generator(_) => branching.and_then(ResolvedBranching::partitions),
            _ => None,
        }
    }

    /// Whether the scheduled identities match the current declaration together with `holding`,
    /// the units a partition transition of this node's branch keeps scheduled beyond the current
    /// count. Ingestor units also depend on resolved construction references and are validated by
    /// their entrypoint plan.
    pub fn units_match_declaration(&self, holding: &[Unit]) -> bool {
        if matches!(self.config.as_ref(), Model::Ingestor(_)) {
            return true;
        }
        let declared = match Self::branch_partitions_for(
            self.config.as_ref(),
            self.resolved_branching.as_ref(),
        ) {
            Some(partitions) => partitions.units(),
            None => vec![Unit::SINGLE],
        };
        let Some(expected) = declared.len().checked_add(holding.len()) else {
            return false;
        };
        if self.units.len() != expected {
            return false;
        }
        for unit in declared.iter().chain(holding) {
            if !self.units.contains_key(unit) {
                return false;
            }
        }
        true
    }

    /// Whether this node keeps durable state per branch key, which a change of its branch's
    /// partition count moves between its units: a deduplicator, window or WASM processor, an
    /// inferencer that declares state, or a materialized relay.
    pub fn keeps_state_per_key(&self) -> bool {
        Model::keeps_state_per_key(self.config.as_ref())
    }

    /// The branch whose partition count decides this node's units: none for a node a branch's
    /// partitions do not divide.
    pub fn partition_branch(&self) -> Option<&BranchName> {
        Self::branch_partitions_for(self.config.as_ref(), self.resolved_branching.as_ref())?;
        self.resolved_branching.as_ref()?.branch()
    }

    /// Adds `holding`, the units a partition transition keeps scheduled beyond the current count,
    /// after this node's declared units. A unit already scheduled keeps its placement.
    #[must_use]
    pub fn with_holding_units(mut self, holding: &[Unit]) -> Self {
        for unit in holding {
            if !self.units.contains_key(unit) {
                let scheduled = ScheduledUnit::for_model(self.config.as_ref());
                self.units.insert(*unit, scheduled);
            }
        }
        self
    }

    /// Whether this node is scheduled as several partition units.
    pub fn is_partitioned(&self) -> bool {
        self.units
            .keys()
            .any(|unit| matches!(unit.kind(), crate::UnitKind::Partition(_)))
    }

    /// The placement of `unit`, when this node is scheduled as it.
    pub fn unit(&self, unit: Unit) -> Option<&ScheduledUnit> {
        self.units.get(&unit)
    }

    /// Places `unit` on `assigned`, with `primary` owning its state. A unit this node is not
    /// scheduled as is left alone and reported as not placed.
    pub fn place_unit(
        &mut self,
        unit: Unit,
        primary: Option<ClusterNodeName>,
        assigned: Vec<ClusterNodeName>,
    ) -> bool {
        let Some(scheduled) = self.units.get_mut(&unit) else {
            return false;
        };
        scheduled.primary_node = primary;
        scheduled.assigned_nodes = assigned;
        true
    }

    /// The Kafka partitions the leader observed for this node, and how they are shared out.
    #[must_use]
    pub fn with_kafka_partitions(mut self, partitions: KafkaPartitionSchedule) -> Self {
        self.kafka_partition_schedule = Some(partitions);
        self
    }

    /// The cluster nodes every unit of this entry is placed on, with `primary` owning its state.
    #[must_use]
    pub fn placed_on(
        mut self,
        primary: Option<ClusterNodeName>,
        assigned: Vec<ClusterNodeName>,
    ) -> Self {
        for unit in self.units.values_mut() {
            unit.primary_node = primary.clone();
            unit.assigned_nodes = assigned.clone();
        }
        self
    }

    /// The unit of a node with exactly one execution unit, regardless of its unit kind.
    pub fn single_unit(&self) -> &ScheduledUnit {
        self.sole_unit()
            .assured("this operation selects a node with exactly one execution unit")
    }

    /// The unit of a node with exactly one execution unit, mutably.
    pub fn single_unit_mut(&mut self) -> &mut ScheduledUnit {
        self.sole_unit_mut()
            .assured("this operation selects a node with exactly one execution unit")
    }

    /// The first unit of this node in unit order.
    pub fn first_unit(&self) -> &ScheduledUnit {
        self.units
            .values()
            .next()
            .assured("every scheduled node is scheduled as at least one unit")
    }

    /// Places every unit of this node on `assigned`, with `primary` owning its state.
    pub fn place_every_unit(
        &mut self,
        primary: Option<ClusterNodeName>,
        assigned: Vec<ClusterNodeName>,
    ) {
        for unit in self.units.values_mut() {
            unit.primary_node = primary.clone();
            unit.assigned_nodes = assigned.clone();
        }
    }

    /// Records `transition` as the ownership move of every unit of this node.
    pub fn record_ownership_transition_of_every_unit(&mut self, transition: &OwnershipTransition) {
        for unit in self.units.values_mut() {
            unit.ownership_transition = Some(transition.clone());
        }
    }

    /// The units of this node that execute on `node_id`, in unit order: each unit whose primary,
    /// or whose one assigned node when it has no primary, is `node_id`. None for a node that runs
    /// on every cluster node.
    pub fn units_executing_on(&self, node_id: &ClusterNodeName) -> Vec<Unit> {
        if self.executes_on_every_cluster_node() {
            return Vec::new();
        }
        let mut units = Vec::new();
        for (unit, scheduled) in &self.units {
            if scheduled.execution_node() == Some(node_id) {
                units.push(*unit);
            }
        }
        units
    }

    /// The units of this node assigned to `node_id`, as their primary or a replica, in unit order.
    pub fn units_assigned_to(&self, node_id: &ClusterNodeName) -> Vec<Unit> {
        let mut units = Vec::new();
        for (unit, scheduled) in &self.units {
            if scheduled.is_assigned_to(node_id) {
                units.push(*unit);
            }
        }
        units
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
        self.sole_unit()?.wasm_state_generations.as_ref()
    }

    /// The latest coordinated guest-state reset this WASM processor published.
    pub fn wasm_state_reset(&self) -> Option<&WasmStateReset> {
        self.sole_unit()?.wasm_state_reset.as_ref()
    }

    /// The recovery attempts this WASM processor's refused guest-state lifetimes have spent.
    pub fn wasm_state_recoveries(&self) -> Option<&WasmStateRecoveries> {
        self.sole_unit()?.wasm_state_recoveries.as_ref()
    }

    /// The execution unit of a concrete branch, using the same fingerprint seed as data routing.
    pub fn unit_of_branch(&self, branch: Option<&BranchKeyFingerprint>) -> Option<Unit> {
        let unit = match self
            .resolved_branching
            .as_ref()
            .and_then(|branching| branching.partitions())
        {
            Some(partitions) if partitions.is_partitioned() => {
                partitions.unit_of(branch?.partition_seed())
            }
            _ => Unit::SINGLE,
        };
        self.units.contains_key(&unit).then_some(unit)
    }

    pub fn wasm_state_generations_of_branch(
        &self,
        branch: Option<&BranchKeyFingerprint>,
    ) -> Option<&WasmStateGenerations> {
        self.units
            .get(&self.unit_of_branch(branch)?)?
            .wasm_state_generations
            .as_ref()
    }

    /// The concrete units selected by one reset, in unit order.
    pub fn units_for_wasm_state_scope(&self, scope: &WasmStateResetScope) -> Vec<Unit> {
        self.units
            .keys()
            .copied()
            .filter(|unit| scope.selects_unit(*unit, self.resolved_branching.as_ref()))
            .collect()
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
        let identity = match &scope {
            WasmStateResetScope::Branch(branch) => self.unit_of_branch(Some(branch))?,
            WasmStateResetScope::Unbranched => self.unit_of_branch(None)?,
            WasmStateResetScope::AllBranches => return None,
        };
        let recoveries = self
            .units
            .get_mut(&identity)?
            .wasm_state_recoveries
            .as_mut()?;
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
        let units = self.units_for_wasm_state_scope(scope);
        let [identity] = units.as_slice() else {
            return false;
        };
        let Some(unit) = self.units.get_mut(identity) else {
            return false;
        };
        let Some(recoveries) = unit.wasm_state_recoveries.as_mut() else {
            return false;
        };
        recoveries.settle(scope, request, outcome)
    }

    /// Continue the guest-state lifetimes `existing` published for this same node.
    ///
    /// A generation belongs to the pinned module binding it was published for. When this entry
    /// binds a different resource version or file than `existing`, the lifetimes continue from
    /// `existing` and every branch then starts a new one, so no snapshot saved under the previous
    /// binding can attach to this one. Every execution unit continues only the lifetimes of its
    /// matching unit; a newly declared unit keeps its own initial lifetimes.
    pub fn continue_wasm_state_generations_of(&mut self, existing: &Self) {
        let (Some(processor), Some(existing_processor)) =
            (self.wasm_processor(), existing.wasm_processor())
        else {
            return;
        };
        let binding_changed = processor.resource != existing_processor.resource
            || processor.resource_version != existing_processor.resource_version
            || processor.file != existing_processor.file;
        for (identity, unit) in &mut self.units {
            let Some(existing_unit) = existing.units.get(identity) else {
                continue;
            };
            let Some(existing_generations) = existing_unit.wasm_state_generations.as_ref() else {
                continue;
            };
            let mut generations = existing_generations.clone();
            if binding_changed {
                generations.begin_every_branch();
            }
            unit.wasm_state_generations = Some(generations);
            unit.wasm_state_reset = if binding_changed {
                None
            } else {
                existing_unit.wasm_state_reset.clone()
            };
            // A recovery attempt belongs to its unit's admitted lifetime. A changed binding
            // restarts that lifetime, so its preceding attempts no longer name a refusal.
            unit.wasm_state_recoveries = match binding_changed {
                true => Some(WasmStateRecoveries::empty()),
                false => existing_unit.wasm_state_recoveries.clone(),
            };
        }
    }

    /// Start a new guest-state lifetime for every branch of this WASM processor, including branches
    /// that exist only as persisted snapshots. Every other kind of node has no guest state, a relay
    /// scheduled as partitions included.
    pub fn begin_wasm_state_generation(&mut self) {
        for unit in self.units.values_mut() {
            if let Some(generations) = unit.wasm_state_generations.as_mut() {
                generations.begin_every_branch();
            }
        }
    }

    /// Start a new guest-state lifetime only for the unit whose owner state is replaced.
    pub fn begin_wasm_unit_state_generation(&mut self, identity: Unit) {
        if let Some(unit) = self.units.get_mut(&identity)
            && let Some(generations) = unit.wasm_state_generations.as_mut()
        {
            generations.begin_every_branch();
        }
    }

    /// Start a new guest-state lifetime for one concrete branch of this WASM processor, leaving every
    /// other branch in the lifetime it has.
    pub fn begin_wasm_branch_state_generation(&mut self, branch: BranchKeyFingerprint) {
        let Some(identity) = self.unit_of_branch(Some(&branch)) else {
            return;
        };
        let Some(unit) = self.units.get_mut(&identity) else {
            return;
        };
        if let Some(generations) = unit.wasm_state_generations.as_mut() {
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
        let selected = self.units_for_wasm_state_scope(&scope);
        let mut began = false;
        for identity in selected {
            let unit = self
                .units
                .get_mut(&identity)
                .verified("the selected unit belongs to this node");
            let Some(generations) = unit.wasm_state_generations.as_mut() else {
                continue;
            };
            if unit
                .wasm_state_reset
                .as_ref()
                .is_some_and(|reset| reset.request() == &request && reset.scope() == &scope)
            {
                continue;
            }
            generations.begin_reset(&scope);
            unit.wasm_state_reset =
                Some(WasmStateReset::publishing(request.clone(), scope, reason));
            began = true;
        }
        began
    }

    /// Mark the initial checkpoint of this exact reset durable and ready for runtime admission.
    pub fn complete_wasm_state_reset(&mut self, request: &CommandExecutionReference) -> bool {
        let mut completed = false;
        for unit in self.units.values_mut() {
            let Some(reset) = unit.wasm_state_reset.as_mut() else {
                continue;
            };
            if reset.request() != request || reset.phase() == WasmStateResetPhase::Ready {
                continue;
            }
            reset.mark_ready();
            completed = true;
        }
        completed
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
        if let Model::Inferencer(model) = self.config.as_ref()
            && !model.state.is_empty()
        {
            components.push(OwnershipStateComponent::Inferencer);
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

    /// Whether any unit of this node is assigned to `node_id`, as its primary or a replica.
    pub fn is_assigned_to(&self, node_id: &ClusterNodeName) -> bool {
        self.units.values().any(|unit| unit.is_assigned_to(node_id))
    }

    /// The one execution unit of a node, or `None` when its units have distinct placements.
    pub fn sole_unit(&self) -> Option<&ScheduledUnit> {
        match self.units.len() {
            1 => self.units.values().next(),
            _ => None,
        }
    }

    /// The one execution unit of a node, mutably, or `None` for several units.
    pub fn sole_unit_mut(&mut self) -> Option<&mut ScheduledUnit> {
        match self.units.len() {
            1 => self.units.values_mut().next(),
            _ => None,
        }
    }

    /// The one node a single-unit node is assigned to, when it is assigned to exactly one. `None`
    /// for a node scheduled as several units.
    pub fn assigned_single_node(&self) -> Option<&ClusterNodeName> {
        match self.sole_unit()?.assigned_nodes.as_slice() {
            [node_id] => Some(node_id),
            _ => None,
        }
    }

    /// The primary of a single-unit node. `None` for a node scheduled as several units.
    pub fn primary_node(&self) -> Option<&ClusterNodeName> {
        self.sole_unit()?.primary_node.as_ref()
    }

    /// The replicas of a single-unit node. Empty for a node scheduled as several units.
    pub fn replica_nodes(&self) -> Vec<&ClusterNodeName> {
        match self.sole_unit() {
            Some(unit) => unit.replica_nodes(),
            None => Vec::new(),
        }
    }

    /// Whether any unit of this node is primary on `node_id`.
    pub fn is_primary_on(&self, node_id: &ClusterNodeName) -> bool {
        self.units.values().any(|unit| unit.is_primary_on(node_id))
    }

    /// The cluster node a single-unit node executes on: its primary, or its one assigned node
    /// when it has no primary. `None` for a node that runs on every cluster node, and for a node
    /// scheduled as several units, which have no one execution node.
    pub fn execution_node(&self) -> Option<&ClusterNodeName> {
        if self.executes_on_every_cluster_node() {
            None
        } else {
            self.primary_node().or_else(|| self.assigned_single_node())
        }
    }

    /// The cluster node `unit` executes on: its primary, or its one assigned node when it has no
    /// primary. `None` for a node that runs on every cluster node, and for a unit this node is not
    /// scheduled as.
    pub fn unit_execution_node(&self, unit: Unit) -> Option<&ClusterNodeName> {
        if self.executes_on_every_cluster_node() {
            return None;
        }
        self.units.get(&unit)?.execution_node()
    }

    /// Whether any unit of this node executes on `node_id`.
    pub fn executes_on(&self, node_id: &ClusterNodeName) -> bool {
        if self.executes_on_every_cluster_node() {
            self.is_assigned_to(node_id)
        } else {
            self.is_primary_on(node_id)
        }
    }

    /// Whether `unit` of this node executes on `node_id`.
    pub fn unit_executes_on(&self, unit: Unit, node_id: &ClusterNodeName) -> bool {
        let Some(scheduled) = self.units.get(&unit) else {
            return false;
        };
        if self.executes_on_every_cluster_node() {
            scheduled.is_assigned_to(node_id)
        } else {
            scheduled.is_primary_on(node_id)
        }
    }

    /// Every node [`Self::executes_on`] accepts, once each, across every unit: each assigned node
    /// of an entity that runs on every cluster node, and otherwise each unit's primary, or each of
    /// its assigned nodes when it has no primary.
    pub fn execution_nodes(&self) -> Vec<ClusterNodeName> {
        let mut nodes = BTreeSet::new();
        for unit in self.units.values() {
            if self.executes_on_every_cluster_node() {
                nodes.extend(unit.assigned_nodes.iter().cloned());
                continue;
            }
            match &unit.primary_node {
                Some(primary) => {
                    nodes.insert(primary.clone());
                }
                None => nodes.extend(unit.assigned_nodes.iter().cloned()),
            }
        }
        nodes.into_iter().collect()
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
